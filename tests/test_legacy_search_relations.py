from typing import Any
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.errors import InsufficientPrivilege, ObjectNotInPrerequisiteState
from psycopg.types.json import Jsonb

from braincore_memory.legacy_lexical_projection import backfill_batch
from braincore_memory.legacy_search import configure_authenticated_readers

from .test_legacy_lexical_projection import (
    _seed,
    _source,
    _tenant,
)
from .test_legacy_lexical_projection import (
    legacy_lexical_database as _legacy_lexical_database,
)
from .test_legacy_search import _credential

legacy_lexical_database = _legacy_lexical_database


def _ready(conn: psycopg.Connection[dict[str, Any]]) -> None:
    while not backfill_batch(conn, batch_size=2)["ready"]:
        pass


def _set_attributes(
    conn: psycopg.Connection[dict[str, Any]], memory: UUID, attributes: dict[str, Any]
) -> None:
    conn.execute(
        """UPDATE braincore.memories SET legacy_attributes=%s,legacy_attributes_ready=true,
           legacy_content_hash=public.digest(content,'sha256') WHERE id=%s""",
        (Jsonb(attributes), memory),
    )


def _assert_parity(conn: psycopg.Connection[dict[str, Any]], kind: str) -> None:
    fields = (
        ["entity_id", "canonical_name", "aliases"]
        if kind == "entity"
        else [
            "fact_id",
            "subject_entity_id",
            "object_entity_id",
            "episode_id",
            "confidence",
            "last_seen_at",
            "valid_from",
            "valid_to",
            "scope_path",
            "current_status",
        ]
    )
    selected = sql.SQL(",").join(sql.Identifier(field) for field in fields)
    relation = "compat_search_entities" if kind == "entity" else "compat_search_facts"
    actual = conn.execute(
        sql.SQL("SELECT {} FROM braincore.{} ORDER BY {}").format(
            selected, sql.Identifier(relation), sql.Identifier(kind + "_id")
        )
    ).fetchall()
    expected = conn.execute(
        sql.SQL("SELECT {} FROM preserve.{} ORDER BY {}").format(
            selected, sql.Identifier(kind), sql.Identifier(kind + "_id")
        )
    ).fetchall()
    assert actual == expected


def test_narrow_relations_match_effective_overrides_hash_nulls_and_lifecycle(
    legacy_lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = legacy_lexical_database
    tenant = _tenant(conn)
    key = _credential(conn, tenant)
    subject, object_entity, episode = uuid4(), uuid4(), uuid4()
    facts: list[UUID] = []
    for state in ("active", "suppressed", "retired"):
        memory = _seed(conn, tenant, state=state)
        _set_attributes(
            conn,
            memory,
            {
                "subject_entity_id": str(subject),
                "object_entity_id": str(object_entity),
                "episode_id": str(episode),
                "confidence": "0.73",
                "last_seen_at": "2020-04-09T12:00:00Z",
                "current_status": "retracted",
            },
        )
        facts.append(memory)
    for attributes in (
        {"aliases": ["alternate", "second"]},
        {"aliases": None},
        {"aliases": ["hidden"], "__braincore_sql_null_columns": ["aliases"]},
        {},
    ):
        entity = _seed(conn, tenant, object_type="entity", content="Canonical entity")
        _set_attributes(conn, entity, attributes)
    _ready(conn)
    for history in (False, True):
        with conn.transaction():
            configure_authenticated_readers(conn, [key], include_inactive=history)
            _assert_parity(conn, "entity")
            _assert_parity(conn, "fact")
    # Current extension overrides win without changing the canonical content.
    conn.execute(
        """INSERT INTO braincore.legacy_client_attributes
           (memory_id,tenant_id,attributes,content_hash,current_override)
           SELECT id,tenant_id,%s,public.digest(content,'sha256'),true
           FROM braincore.memories WHERE id=%s""",
        (Jsonb({"subject_entity_id": str(object_entity), "confidence": "0.9"}), facts[0]),
    )
    with conn.transaction():
        configure_authenticated_readers(conn, [key], include_inactive=True)
        _assert_parity(conn, "fact")
    conn.execute("DELETE FROM braincore.legacy_client_attributes WHERE memory_id=%s", (facts[0],))
    # Same-lexeme revision invalidates object references, not subject/episode fields.
    conn.execute("UPDATE braincore.memories SET content=content || '!' WHERE id=%s", (facts[0],))
    with conn.transaction():
        configure_authenticated_readers(conn, [key], include_inactive=True)
        _assert_parity(conn, "fact")
        row = conn.execute(
            "SELECT subject_entity_id,object_entity_id,episode_id "
            "FROM braincore.compat_search_facts WHERE native_id=%s",
            (facts[0],),
        ).fetchone()
        assert row == {
            "subject_entity_id": subject,
            "object_entity_id": None,
            "episode_id": episode,
        }


def test_attribute_only_updates_ids_and_canonical_moves_refresh_only_affected_rows(
    legacy_lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = legacy_lexical_database
    tenant, other = _tenant(conn), _tenant(conn)
    fact, untouched = _seed(conn, tenant), _seed(conn, tenant)
    _ready(conn)
    original = conn.execute(
        "SELECT xmin::text AS version FROM braincore.memories_lexical WHERE id=%s", (untouched,)
    ).fetchone()
    external, changed_external = uuid4(), uuid4()
    conn.execute(
        "INSERT INTO braincore.legacy_client_ids(memory_id,tenant_id,object_type,external_id) "
        "VALUES(%s,%s,'fact',%s)",
        (fact, tenant, external),
    )
    assert conn.execute(
        "SELECT compat_external_id FROM braincore.memories_lexical WHERE id=%s", (fact,)
    ).fetchone() == {"compat_external_id": external}
    conn.execute(
        "UPDATE braincore.legacy_client_ids SET external_id=%s WHERE memory_id=%s",
        (changed_external, fact),
    )
    _set_attributes(conn, fact, {"subject_entity_id": str(uuid4()), "confidence": "0.2"})
    conn.execute(
        """INSERT INTO braincore.legacy_client_attributes
           (memory_id,tenant_id,attributes,content_hash,current_override)
           SELECT id,tenant_id,'{}'::jsonb,public.digest(content,'sha256'),true
           FROM braincore.memories WHERE id=%s""",
        (fact,),
    )
    subject = uuid4()
    conn.execute(
        "UPDATE braincore.legacy_client_attributes SET attributes=%s WHERE memory_id=%s",
        (Jsonb({"subject_entity_id": str(subject), "confidence": "0.8"}), fact),
    )
    assert conn.execute(
        "SELECT compat_subject_entity_id FROM braincore.memories_lexical WHERE id=%s", (fact,)
    ).fetchone() == {"compat_subject_entity_id": subject}
    conn.execute("DELETE FROM braincore.legacy_client_attributes WHERE memory_id=%s", (fact,))
    conn.execute("DELETE FROM braincore.legacy_client_ids WHERE memory_id=%s", (fact,))
    changed = uuid4()
    conn.execute(
        "UPDATE braincore.memories SET id=%s,tenant_id=%s WHERE id=%s", (changed, other, fact)
    )
    assert (
        conn.execute("SELECT id FROM braincore.memories_lexical WHERE id=%s", (fact,)).fetchone()
        is None
    )
    assert conn.execute(
        "SELECT tenant_id,compat_external_id FROM braincore.memories_lexical WHERE id=%s",
        (changed,),
    ).fetchone() == {"tenant_id": other, "compat_external_id": changed}
    assert (
        conn.execute(
            "SELECT xmin::text AS version FROM braincore.memories_lexical WHERE id=%s", (untouched,)
        ).fetchone()
        == original
    )
    key = _credential(conn, other)
    with conn.transaction():
        configure_authenticated_readers(conn, [key])
        _assert_parity(conn, "fact")


def test_explicit_default_reader_scope_and_history_visibility_match_original_views(
    legacy_lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = legacy_lexical_database
    tenants = {
        name: conn.execute(
            "INSERT INTO braincore.tenants(name) VALUES(%s) RETURNING id", (name,)
        ).fetchone()["id"]
        for name in ("selected", "default", "unrelated")
    }
    keys = {name: _credential(conn, tenant) for name, tenant in tenants.items()}
    for tenant in tenants.values():
        for kind in ("entity", "fact"):
            _seed(conn, tenant, object_type=kind)
            _seed(conn, tenant, object_type=kind, state="suppressed")
            _seed(conn, tenant, object_type=kind, user_id="bob", scope=Jsonb({"user_id": "bob"}))
    _ready(conn)
    for names in (["selected"], ["selected", "default"]):
        for history in (False, True):
            with conn.transaction():
                configure_authenticated_readers(
                    conn, [keys[name] for name in names], include_inactive=history
                )
                _assert_parity(conn, "entity")
                _assert_parity(conn, "fact")
                visible = {
                    row["tenant_id"]
                    for row in conn.execute(
                        "SELECT tenant_id FROM braincore.compat_search_facts"
                    ).fetchall()
                }
                assert visible == {tenants[name] for name in names}
    scoped = [
        _credential(conn, tenants[name], roles=["read"], scope={"user_id": "alice"})
        for name in ("selected", "default")
    ]
    with conn.transaction():
        configure_authenticated_readers(conn, scoped)
        _assert_parity(conn, "fact")
        assert conn.execute(
            "SELECT count(*) AS n FROM braincore.compat_search_facts"
        ).fetchone() == {"n": 2}


def test_existing_projection_upgrade_resets_readiness_and_backfill_populates_relations(
    legacy_lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = legacy_lexical_database
    tenant = _tenant(conn)
    fact = _seed(conn, tenant)
    _set_attributes(conn, fact, {"confidence": "0.7"})
    key = _credential(conn, tenant)
    # An initialized legacy vector is insufficient until new relation fields are copied.
    conn.execute(
        "UPDATE braincore.memories_lexical SET compat_external_id=NULL WHERE id=%s", (fact,)
    )
    with conn.transaction():
        configure_authenticated_readers(conn, [key])
        with pytest.raises(ObjectNotInPrerequisiteState), conn.transaction():
            conn.execute("SELECT * FROM braincore.compat_search_facts").fetchall()
    first = backfill_batch(conn, batch_size=1)
    assert not first["ready"] and first["scanned_items"] == 1
    assert backfill_batch(conn, batch_size=1)["ready"]
    with conn.transaction():
        configure_authenticated_readers(conn, [key])
        _assert_parity(conn, "fact")


def test_bounded_fact_evidence_matches_selected_original_rows_and_owner_guards(
    legacy_lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    conn = legacy_lexical_database
    tenant, other = _tenant(conn), _tenant(conn)
    fact, another, unrelated = _seed(conn, tenant), _seed(conn, tenant), _seed(conn, other)
    segment = _seed(conn, tenant, object_type="segment")
    source = _source(conn)
    evidence = uuid4()
    payload = {
        "fact_evidence_id": str(evidence),
        "excerpt": "retained evidence",
        "fact_id": str(fact),
    }
    record_key = {"id": str(evidence)}
    conn.execute(
        """INSERT INTO braincore.legacy_record_versions
           (source_id,source_table,record_key,payload,payload_hash)
           VALUES(%s,'fact_evidence',%s,%s,public.digest(%s::jsonb::text,'sha256'))""",
        (source, Jsonb(record_key), Jsonb(payload), Jsonb(payload)),
    )
    conn.execute(
        """INSERT INTO braincore.legacy_fact_evidence
           (source_id,record_key,native_id,tenant_id,native_fact_id,native_segment_id,
            imported_payload_hash,attributes)
           VALUES(%s,%s,%s,%s,%s,%s,public.digest(%s::jsonb::text,'sha256'),%s)""",
        (source, Jsonb(record_key), uuid4(), tenant, fact, segment, Jsonb(payload), Jsonb(payload)),
    )
    for parent, owner in ((fact, tenant), (another, tenant), (unrelated, other)):
        conn.execute(
            "INSERT INTO braincore.legacy_client_controls"
            "(tenant_id,object_type,external_id,scope,fields) "
            "VALUES(%s,'fact_evidence',%s,%s,%s)",
            (
                owner,
                uuid4(),
                Jsonb({"user_id": "alice"}),
                Jsonb({"fact_id": str(parent), "excerpt": "native evidence"}),
            ),
        )
    key = _credential(conn, tenant)
    with conn.transaction():
        configure_authenticated_readers(conn, [key])
        expected = conn.execute(
            "SELECT * FROM preserve.fact_evidence WHERE fact_id=%s ORDER BY fact_evidence_id",
            (fact,),
        ).fetchall()
        actual = conn.execute(
            "SELECT * FROM braincore.compat_hydrate_fact_evidence(%s::uuid[]) "
            "ORDER BY fact_evidence_id",
            ([fact, unrelated],),
        ).fetchall()
        assert actual == expected and len(actual) == 2
        assert (
            conn.execute(
                "SELECT * FROM braincore.compat_hydrate_fact_evidence(%s::uuid[])", ([],)
            ).fetchall()
            == []
        )
    conn.execute("UPDATE braincore.memories SET state='suppressed' WHERE id=%s", (fact,))
    with conn.transaction():
        configure_authenticated_readers(conn, [key])
        assert (
            conn.execute(
                "SELECT * FROM braincore.compat_hydrate_fact_evidence(%s::uuid[])", ([fact],)
            ).fetchall()
            == []
        )
    conn.execute("CREATE TEMP TABLE client_ids_spoof(memory_id uuid)")
    conn.execute(
        "CREATE TRIGGER spoof_sync AFTER INSERT ON client_ids_spoof "
        "FOR EACH ROW EXECUTE FUNCTION braincore.sync_legacy_id_search_projection()"
    )
    with pytest.raises(InsufficientPrivilege, match="canonical client IDs"), conn.transaction():
        conn.execute("INSERT INTO client_ids_spoof VALUES(%s)", (uuid4(),))


def test_upgrade_of_ready_existing_projection_requires_fresh_resumable_backfill(
    legacy_lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    from psycopg.conninfo import make_conninfo
    from psycopg.rows import dict_row

    from braincore_memory.config import get_settings
    from braincore_memory.migrate import migration_root

    database = "braincore_relation_upgrade_test_" + uuid4().hex
    dsn = get_settings().database_url.get_secret_value()
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as admin:
        assert (
            admin.execute("SELECT current_database() AS name").fetchone()["name"]
            == "braincore_test"
        )
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        try:
            with psycopg.connect(
                make_conninfo(dsn, dbname=database), autocommit=True, row_factory=dict_row
            ) as conn:
                for migration in sorted(migration_root().glob("*.sql")):
                    if int(migration.name[:3]) <= 23:
                        with conn.transaction():
                            conn.execute(migration.read_text())
                tenant = _tenant(conn)
                memories = [_seed(conn, tenant) for _ in range(2)]
                _ready(conn)
                assert conn.execute(
                    "SELECT ready FROM braincore.legacy_lexical_projection_state"
                ).fetchone() == {"ready": True}
                with conn.transaction():
                    conn.execute((migration_root() / "024_legacy_search_relations.sql").read_text())
                assert conn.execute(
                    "SELECT ready,rows_processed,backfill_after_id "
                    "FROM braincore.legacy_lexical_projection_state"
                ).fetchone() == {"ready": False, "rows_processed": 0, "backfill_after_id": None}
                assert backfill_batch(conn, batch_size=1)["scanned_items"] == 1
                # Resume from the committed checkpoint; triggers retain concurrent writes.
            with psycopg.connect(
                make_conninfo(dsn, dbname=database), autocommit=True, row_factory=dict_row
            ) as conn:
                _ready(conn)
                rows = conn.execute(
                    "SELECT id,compat_external_id FROM braincore.memories_lexical ORDER BY id"
                ).fetchall()
                assert rows == [
                    {"id": memory, "compat_external_id": memory} for memory in sorted(memories)
                ]
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))


def test_structured_and_temporal_helpers_match_original_preserve_selection(
    legacy_lexical_database: psycopg.Connection[dict[str, Any]],
) -> None:
    from datetime import UTC, datetime

    from braincore_memory.legacy_search import (
        resolve_visible_fact_ids,
        select_related_candidates,
        select_structured_candidates,
    )

    conn = legacy_lexical_database
    tenant, other = _tenant(conn), _tenant(conn)
    key = _credential(conn, tenant, roles=["read"], scope={"user_id": "alice"})
    entity = _seed(conn, tenant, object_type="entity", content="Unmatched canonical name")
    entity_alias = uuid4()
    conn.execute(
        "INSERT INTO braincore.legacy_client_ids "
        "(memory_id,tenant_id,object_type,external_id) VALUES(%s,%s,'entity',%s)",
        (entity, tenant, entity_alias),
    )
    _set_attributes(conn, entity, {"aliases": ["needle alias"]})
    episode = uuid4()
    for owner, confidence, project, state, start in (
        (tenant, "0.9", "project/a", "active", 2000),
        (tenant, "0.7", "project/a", "active", 2000),
        (tenant, "0.8", "project/a", "active", 2100),
        (tenant, "0.99", "elsewhere", "active", 2000),
        (tenant, "0.95", "project/a", "suppressed", 2000),
        (other, "1.0", "project/a", "active", 2000),
    ):
        fact = _seed(
            conn,
            owner,
            project_id=project,
            scope=Jsonb({"user_id": "alice", "project_id": project}),
            state=state,
            valid_from=datetime(start, 1, 1, tzinfo=UTC),
        )
        _set_attributes(
            conn,
            fact,
            {
                "subject_entity_id": str(entity_alias),
                "episode_id": str(episode),
                "confidence": confidence,
                "last_seen_at": "2021-01-01T00:00:00Z",
            },
        )
    _ready(conn)
    for as_of in (None, "2020-01-01T00:00:00Z"):
        with conn.transaction():
            configure_authenticated_readers(conn, [key], include_inactive=as_of is not None)
            validity = (
                ""
                if as_of is None
                else " AND (f.valid_from IS NULL OR f.valid_from<=%s::timestamptz) "
                "AND (f.valid_to IS NULL OR f.valid_to>%s::timestamptz)"
            )
            parameters = ["%needle%", "%needle%", "project/a%"] + (
                [] if as_of is None else [as_of, as_of]
            )
            original = conn.execute(
                sql.SQL(
                    "WITH matched_entities AS (SELECT entity_id FROM preserve.entity "
                    "WHERE canonical_name ILIKE %s OR aliases::text ILIKE %s LIMIT 20) "
                    "SELECT f.fact_id FROM preserve.fact f JOIN matched_entities me "
                    "ON f.subject_entity_id=me.entity_id WHERE f.current_status='active' "
                    "AND f.scope_path LIKE %s {validity} "
                    "ORDER BY f.confidence DESC,f.last_seen_at DESC NULLS LAST LIMIT 30"
                ).format(validity=sql.SQL(validity)),
                parameters,
            ).fetchall()
            expected = [
                row["fact_id"] for row in original
            ]  # These facts use canonical IDs as external IDs.
            assert (
                select_structured_candidates(conn, "needle", scope="project/a", as_of=as_of)
                == expected
            )
            assert (
                select_structured_candidates(
                    conn, "no-matching-entity", scope="project/a", as_of=as_of
                )
                == []
            )
            assert set(resolve_visible_fact_ids(conn, [str(item) for item in expected])) == set(
                expected
            )
            first = expected[0]
            related = conn.execute(
                sql.SQL(
                    "SELECT DISTINCT f2.fact_id FROM preserve.fact f1 JOIN preserve.fact f2 "
                    "ON f2.subject_entity_id=f1.subject_entity_id AND f2.fact_id<>f1.fact_id "
                    "WHERE f1.fact_id=%s AND f2.current_status='active' "
                    "AND f2.scope_path LIKE %s {validity} LIMIT 20"
                ).format(validity=sql.SQL(validity.replace("f.", "f2."))),
                [first, "project/a%"] + ([] if as_of is None else [as_of, as_of]),
            ).fetchall()
            assert set(
                select_related_candidates(
                    conn, fact_ids=[str(first)], scope="project/a", as_of=as_of
                )
            ) == {row["fact_id"] for row in related}
            episode_rows = conn.execute(
                sql.SQL(
                    "SELECT f.fact_id FROM preserve.fact f WHERE f.episode_id=%s "
                    "AND f.current_status='active' AND f.scope_path LIKE %s {validity} LIMIT 20"
                ).format(validity=sql.SQL(validity)),
                [episode, "project/a%"] + ([] if as_of is None else [as_of, as_of]),
            ).fetchall()
            assert set(
                select_related_candidates(
                    conn, episode_ids=[str(episode)], scope="project/a", as_of=as_of
                )
            ) == {row["fact_id"] for row in episode_rows}
