import hashlib
import os
from collections.abc import Iterator
from typing import Any
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.errors import InsufficientPrivilege, LockNotAvailable
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from braincore_memory.auth import Principal
from braincore_memory.config import get_settings
from braincore_memory.legacy_clients import configure_legacy_read
from braincore_memory.legacy_lexical_projection import backfill_batch, backfill_projection
from braincore_memory.lexical_projection import backfill_batch as native_backfill_batch
from braincore_memory.migrate import migration_root


@pytest.fixture
def legacy_lexical_database() -> Iterator[psycopg.Connection[dict[str, Any]]]:
    if os.getenv("BRAINCORE_E2E") != "1":
        pytest.skip("requires the disposable integration database")
    dsn = get_settings().database_url.get_secret_value()
    database = "braincore_legacy_lexical_test_" + uuid4().hex
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as admin:
        assert admin.execute("SELECT current_database() AS name").fetchone()["name"] == (
            "braincore_test"
        )
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        try:
            with psycopg.connect(
                make_conninfo(dsn, dbname=database), autocommit=True, row_factory=dict_row
            ) as conn:
                for path in sorted(migration_root().glob("*.sql")):
                    with conn.transaction():
                        conn.execute(path.read_text())
                yield conn
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))


def _tenant(conn: psycopg.Connection[dict[str, Any]]) -> UUID:
    return conn.execute(
        "INSERT INTO braincore.tenants(name) VALUES(%s) RETURNING id", (uuid4().hex,)
    ).fetchone()["id"]


def _seed(conn: psycopg.Connection[dict[str, Any]], tenant: UUID, **overrides: Any) -> UUID:
    memory_id = overrides.pop("id", uuid4())
    fields = {
        "id": memory_id,
        "tenant_id": tenant,
        "content": "native alpha beta",
        "dedupe_key": hashlib.sha256(memory_id.bytes).digest(),
        "user_id": "alice",
        "scope": Jsonb({"user_id": "alice", "project_id": None}),
        "object_type": "fact",
        **overrides,
    }
    conn.execute(
        sql.SQL("INSERT INTO braincore.memories ({}) VALUES ({})").format(
            sql.SQL(",").join(sql.Identifier(field) for field in fields),
            sql.SQL(",").join(sql.Placeholder() for _ in fields),
        ),
        list(fields.values()),
    )
    return memory_id


def _source(conn: psycopg.Connection[dict[str, Any]]) -> UUID:
    source = uuid4()
    conn.execute(
        "INSERT INTO braincore.legacy_sources(id,source_schema,key_mapping) VALUES(%s,'test',%s)",
        (source, Jsonb({})),
    )
    return source


def _imported(
    conn: psycopg.Connection[dict[str, Any]], tenant: UUID, source: UUID, object_type: str
) -> UUID:
    memory_id = uuid4()
    return _seed(
        conn,
        tenant,
        id=memory_id,
        object_type=object_type,
        legacy_source_id=source,
        legacy_record_key=Jsonb({"id": str(memory_id)}),
        legacy_external_id=memory_id,
    )


def _attributes(
    conn: psycopg.Connection[dict[str, Any]],
    memory: UUID,
    *,
    vector_text: str | None = "extension gamma",
    content: str = "native alpha beta",
    override: bool = False,
) -> None:
    conn.execute(
        """INSERT INTO braincore.legacy_client_attributes
           (memory_id,tenant_id,attributes,content_hash,search_vector,current_override)
           SELECT id,tenant_id,%s,%s,to_tsvector('english',%s),%s
           FROM braincore.memories WHERE id=%s
           ON CONFLICT(memory_id) DO UPDATE SET attributes=EXCLUDED.attributes,
           content_hash=EXCLUDED.content_hash,search_vector=EXCLUDED.search_vector,
           current_override=EXCLUDED.current_override""",
        (Jsonb({}), hashlib.sha256(content.encode()).digest(), vector_text, override, memory),
    )


def _assert_view_parity(
    conn: psycopg.Connection[dict[str, Any]], tenant: UUID, memory: UUID, object_type: str
) -> None:
    projected = conn.execute(
        """SELECT l.scope=m.scope AS exact_scope,l.compat_ready,l.compat_fts::text AS fts
           FROM braincore.memories m JOIN braincore.memories_lexical l USING(id,tenant_id)
           WHERE m.id=%s""",
        (memory,),
    ).fetchone()
    assert projected is not None and projected["exact_scope"]
    with conn.transaction():
        configure_legacy_read(conn, Principal(uuid4(), tenant, frozenset({"read", "admin"})))
        view = conn.execute(
            sql.SQL("SELECT fts::text AS fts FROM preserve.{} WHERE {}=%s").format(
                sql.Identifier(object_type), sql.Identifier(object_type + "_id")
            ),
            (memory,),
        ).fetchone()
        assert (view is not None) is projected["compat_ready"]
        if view is not None:
            assert view["fts"] == projected["fts"]


def test_projection_preserves_every_view_fts_case_and_exact_scope(
    legacy_lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = legacy_lexical_database
    tenant = _tenant(conn)
    source = _source(conn)
    for object_type in ("fact", "memory", "segment", "episode"):
        native = _seed(conn, tenant, object_type=object_type)
        _assert_view_parity(conn, tenant, native, object_type)
        memory = _imported(conn, tenant, source, object_type)
        _assert_view_parity(conn, tenant, memory, object_type)  # Unhydrated imports stay hidden.
        _attributes(conn, memory)
        _assert_view_parity(conn, tenant, memory, object_type)
        conn.execute(
            """UPDATE braincore.memories SET legacy_attributes_ready=true,
               legacy_content_hash=public.digest(content,'sha256'),
               legacy_search_vector=to_tsvector('english','hydrated delta') WHERE id=%s""",
            (memory,),
        )
        _assert_view_parity(conn, tenant, memory, object_type)
        _attributes(conn, memory, vector_text="override epsilon", override=True)
        _assert_view_parity(conn, tenant, memory, object_type)
        _attributes(conn, memory, vector_text=None, override=True)
        _assert_view_parity(conn, tenant, memory, object_type)  # NULL falls through to native.
        _attributes(conn, memory, content="stale content", override=True)
        _assert_view_parity(conn, tenant, memory, object_type)
        conn.execute(
            "UPDATE braincore.memories SET legacy_fts_eligible=false WHERE id=%s", (memory,)
        )
        _assert_view_parity(conn, tenant, memory, object_type)


def test_attribute_only_changes_delete_and_content_revision_are_atomic_and_targeted(
    legacy_lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = legacy_lexical_database
    tenant = _tenant(conn)
    memory = _imported(conn, tenant, _source(conn), "fact")
    unrelated = _seed(conn, tenant)
    original = conn.execute(
        "SELECT xmin::text AS version FROM braincore.memories_lexical WHERE id=%s", (unrelated,)
    ).fetchone()
    _attributes(conn, memory, vector_text="accepted gamma", override=True)
    _assert_view_parity(conn, tenant, memory, "fact")
    with conn.transaction(force_rollback=True):
        _attributes(conn, memory, vector_text="uncommitted delta", override=True)
        assert conn.execute(
            "SELECT compat_fts @@ plainto_tsquery('english','delta') AS changed "
            "FROM braincore.memories_lexical WHERE id=%s",
            (memory,),
        ).fetchone() == {"changed": True}
    assert conn.execute(
        "SELECT compat_fts @@ plainto_tsquery('english','gamma') AS retained "
        "FROM braincore.memories_lexical WHERE id=%s",
        (memory,),
    ).fetchone() == {"retained": True}
    # Text punctuation can invalidate the compatibility hash without changing native lexemes.
    conn.execute(
        "UPDATE braincore.memories SET content='native alpha beta!' WHERE id=%s", (memory,)
    )
    _assert_view_parity(conn, tenant, memory, "fact")
    assert conn.execute(
        "SELECT compat_fts=search_vector AS native FROM braincore.memories_lexical WHERE id=%s",
        (memory,),
    ).fetchone() == {"native": True}
    conn.execute("DELETE FROM braincore.legacy_client_attributes WHERE memory_id=%s", (memory,))
    _assert_view_parity(conn, tenant, memory, "fact")
    assert (
        conn.execute(
            "SELECT xmin::text AS version FROM braincore.memories_lexical WHERE id=%s", (unrelated,)
        ).fetchone()
        == original
    )


def test_backfill_resumes_and_certifies_existing_native_rows_without_rewriting_canonical_data(
    legacy_lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = legacy_lexical_database
    tenant = _tenant(conn)
    conn.execute("ALTER TABLE braincore.memories DISABLE TRIGGER memories_lexical_sync")
    historical = [_seed(conn, tenant) for _ in range(3)]
    conn.execute("ALTER TABLE braincore.memories ENABLE TRIGGER memories_lexical_sync")
    native_backfill_batch(conn)
    assert conn.execute(
        "SELECT count(*) AS uninitialized FROM braincore.memories_lexical WHERE scope IS NULL"
    ).fetchone() == {"uninitialized": 3}
    before = conn.execute(
        "SELECT id,xmin::text AS version FROM braincore.memories ORDER BY id"
    ).fetchall()
    dsn = make_conninfo(get_settings().database_url.get_secret_value(), dbname=conn.info.dbname)
    first = backfill_projection(dsn, batch_size=1, max_batches=1)
    assert first["scanned_items"] == 1 and not first["ready"]
    assert first["backfill_after_id"] == min(historical)
    assert (
        conn.execute(
            "SELECT id,xmin::text AS version FROM braincore.memories ORDER BY id"
        ).fetchall()
        == before
    )
    # Inserts behind the checkpoint are synchronized by triggers, not lost by UUID pagination.
    live = _seed(conn, tenant, id=UUID(int=1))
    _attributes(conn, min(historical), vector_text="current delta", override=True)
    result = backfill_projection(dsn, batch_size=1)
    assert result["ready"] and result["rows_processed"] == 3
    assert backfill_batch(conn)["scanned_items"] == 0
    for memory in [*historical, live]:
        _assert_view_parity(conn, tenant, memory, "fact")


def test_backfill_locks_source_and_checkpoint_then_preserves_concurrent_extension_writer(
    legacy_lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = legacy_lexical_database
    tenant = _tenant(conn)
    memory = _seed(conn, tenant)
    _attributes(conn, memory, override=True)
    dsn = make_conninfo(get_settings().database_url.get_secret_value(), dbname=conn.info.dbname)
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as writer:
        writer.execute("SET lock_timeout='100ms'")
        with conn.transaction(force_rollback=True):
            assert backfill_batch(conn, batch_size=1)["scanned_items"] == 1
            with pytest.raises(LockNotAvailable):
                _attributes(writer, memory, vector_text="concurrent delta", override=True)
        assert conn.execute(
            "SELECT backfill_after_id,rows_processed FROM braincore.legacy_lexical_projection_state"
        ).fetchone() == {"backfill_after_id": None, "rows_processed": 0}
        _attributes(writer, memory, vector_text="concurrent delta", override=True)
    assert backfill_projection(dsn, batch_size=1)["ready"]
    _assert_view_parity(conn, tenant, memory, "fact")
    assert conn.execute(
        "SELECT compat_fts @@ plainto_tsquery('english','delta') AS current "
        "FROM braincore.memories_lexical WHERE id=%s",
        (memory,),
    ).fetchone() == {"current": True}


def test_incomplete_compatibility_backfill_never_marks_ready(
    legacy_lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = legacy_lexical_database
    memory = _seed(conn, _tenant(conn))
    assert not backfill_batch(conn)["ready"]
    conn.execute("UPDATE braincore.memories_lexical SET scope=NULL WHERE id=%s", (memory,))
    with pytest.raises(RuntimeError, match="incomplete"):
        backfill_batch(conn)
    assert conn.execute(
        "SELECT ready FROM braincore.legacy_lexical_projection_state"
    ).fetchone() == {"ready": False}


def test_identity_moves_attribute_moves_and_restricted_writer_guard(
    legacy_lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = legacy_lexical_database
    tenant, other = _tenant(conn), _tenant(conn)
    old = _seed(conn, tenant)
    new = uuid4()
    conn.execute("UPDATE braincore.memories SET id=%s,tenant_id=%s WHERE id=%s", (new, other, old))
    assert (
        conn.execute("SELECT id FROM braincore.memories_lexical WHERE id=%s", (old,)).fetchone()
        is None
    )
    assert conn.execute(
        "SELECT tenant_id FROM braincore.memories_lexical WHERE id=%s", (new,)
    ).fetchone() == {"tenant_id": other}
    target = _seed(conn, other)
    _attributes(conn, new, override=True)
    conn.execute(
        "UPDATE braincore.legacy_client_attributes SET memory_id=%s WHERE memory_id=%s",
        (target, new),
    )
    for memory in (new, target):
        _assert_view_parity(conn, other, memory, "fact")
    role = "legacy_lexical_writer_" + uuid4().hex
    conn.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(role)))
    try:
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA braincore TO {}").format(sql.Identifier(role)))
        conn.execute(
            sql.SQL("GRANT SELECT,UPDATE ON braincore.memories TO {}").format(sql.Identifier(role))
        )
        conn.execute(
            sql.SQL("GRANT SELECT,UPDATE ON braincore.legacy_client_attributes TO {}").format(
                sql.Identifier(role)
            )
        )
        with conn.transaction():
            conn.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
            conn.execute(
                "UPDATE braincore.legacy_client_attributes "
                "SET search_vector=to_tsvector('english','restricted delta') WHERE memory_id=%s",
                (target,),
            )
            with pytest.raises(InsufficientPrivilege), conn.transaction():
                conn.execute(
                    "SELECT braincore.refresh_legacy_lexical_projection(%s::uuid[])", ([target],)
                )
            with pytest.raises(InsufficientPrivilege), conn.transaction():
                conn.execute("DELETE FROM braincore.memories_lexical WHERE id=%s", (target,))
        _assert_view_parity(conn, other, target, "fact")
    finally:
        conn.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
        conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
    conn.execute("CREATE TEMP TABLE legacy_attributes_spoof(memory_id uuid)")
    conn.execute(
        "CREATE TRIGGER spoof_sync AFTER INSERT ON legacy_attributes_spoof "
        "FOR EACH ROW EXECUTE FUNCTION braincore.sync_legacy_attribute_lexical_projection()"
    )
    with (
        pytest.raises(InsufficientPrivilege, match="canonical client attributes"),
        conn.transaction(),
    ):
        conn.execute("INSERT INTO legacy_attributes_spoof VALUES(%s)", (uuid4(),))


@pytest.mark.parametrize("batch_size", [0, -1, 10_001])
def test_invalid_batch_size_is_rejected_without_connecting(batch_size: int) -> None:
    with pytest.raises(ValueError, match="batch_size"):
        backfill_projection("unused", batch_size=batch_size)


def test_invalid_batch_limit_is_rejected_without_connecting() -> None:
    with pytest.raises(ValueError, match="max_batches"):
        backfill_projection("unused", max_batches=0)
