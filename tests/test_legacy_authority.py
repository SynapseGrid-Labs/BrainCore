from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from threading import Event
from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row

from braincore_memory import api
from braincore_memory.auth import Principal
from braincore_memory.legacy_authority import guard_import_rows
from braincore_memory.legacy_clients import mutate_legacy_object
from braincore_memory.legacy_evidence import import_artifacts, import_direct_evidence
from braincore_memory.legacy_ledger import copy_source
from braincore_memory.legacy_objects import import_objects, reconcile_objects
from braincore_memory.legacy_provenance import (
    import_auxiliary_objects,
    import_provenance,
    reconcile_provenance,
)
from braincore_memory.legacy_writes import mutate_artifact, mutate_control
from braincore_memory.migrate import migration_root
from braincore_memory.models import LifecycleRequest, RevisionRequest

from .test_legacy_objects import typed_source as _typed_source

typed_source = _typed_source


def principal(fixture: dict[str, Any]) -> Principal:
    c = fixture["target"]
    tenant = c.execute("SELECT id FROM braincore.tenants WHERE name='alpha'").fetchone()["id"]
    key = c.execute(
        "INSERT INTO braincore.api_keys(tenant_id,name,prefix,digest,roles) "
        "VALUES (%s,'authority-test','test-only',%s,ARRAY['read','write','admin']) RETURNING id",
        (tenant, uuid4().bytes + uuid4().bytes),
    ).fetchone()["id"]
    return Principal(key, tenant, frozenset({"read", "write", "admin"}))


def recapture(fixture: dict[str, Any]) -> None:
    with psycopg.connect(fixture["dsn"], autocommit=True, row_factory=dict_row) as c:
        copy_source(c, fixture["target"], fixture["source_id"], "synthetic_legacy")


def memory_state(c: Any, native_id: Any) -> dict[str, Any]:
    return c.execute(
        "SELECT row_to_json(m)::jsonb AS memory, row_to_json(o)::jsonb AS mapping "
        "FROM braincore.memories m LEFT JOIN braincore.legacy_object_map o "
        "ON o.native_memory_id=m.id WHERE m.id=%s",
        (native_id,),
    ).fetchone()


def test_native_core_correction_preserves_replay_and_rejects_changed_source(
    typed_source: dict[str, Any],
) -> None:
    c, sid = typed_source["target"], typed_source["source_id"]
    actor = principal(typed_source)
    result = mutate_legacy_object(
        c,
        actor,
        object_type="segment",
        object_id=typed_source["same_external"],
        fields={"content": "Accepted corrected segment", "line_start": 11},
        idempotency_key="accepted-correction",
    )
    native_id = result["native_id"]
    accepted = memory_state(c, native_id)
    import_objects(c, sid)
    assert memory_state(c, native_id) == accepted
    assert reconcile_objects(c, sid)["typed_semantic_per_key_reconciliation_gate"] == "PASS"
    # Deleting ordinary audit history cannot erase the monotonic authority.
    c.execute("DELETE FROM braincore.audit_events WHERE target_id=%s", (native_id,))
    c.execute(
        "UPDATE synthetic_legacy.segment SET content='Later source change' WHERE segment_id=%s",
        (typed_source["same_external"],),
    )
    recapture(typed_source)
    with pytest.raises(psycopg.errors.CheckViolation, match="source_native_authority_conflict"):
        import_objects(c, sid, changed_only=True)
    assert memory_state(c, native_id) == accepted
    assert (
        c.execute(
            "SELECT count(*) AS n FROM braincore.legacy_native_authority WHERE native_id=%s",
            (native_id,),
        ).fetchone()["n"]
        == 1
    )


async def test_native_revision_and_lifecycle_are_captured_in_accepted_transaction(
    typed_source: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    c, sid = typed_source["target"], typed_source["source_id"]
    actor = principal(typed_source)
    native_id = c.execute(
        "SELECT id FROM braincore.memories WHERE object_type='segment' AND legacy_external_id=%s",
        (typed_source["same_external"],),
    ).fetchone()["id"]

    @asynccontextmanager
    async def connect() -> AsyncIterator[Any]:
        async with await psycopg.AsyncConnection.connect(
            typed_source["dsn"], row_factory=dict_row
        ) as db:
            yield db

    monkeypatch.setattr(api, "connection", connect)
    await api.revise(
        native_id,
        RevisionRequest(content="Native API revision", reason="correction"),
        "native-revision",
        actor,
    )
    await api.lifecycle(
        native_id, LifecycleRequest(action="retire", reason="retirement"), "native-lifecycle", actor
    )
    accepted = memory_state(c, native_id)
    import_objects(
        c,
        sid,
        changed_only=True,
        changed_keys={
            "segment": [
                c.execute(
                    "SELECT record_key::text AS key FROM "
                    "braincore.legacy_object_map WHERE native_memory_id=%s",
                    (native_id,),
                ).fetchone()["key"]
            ]
        },
    )
    assert memory_state(c, native_id) == accepted
    assert accepted["memory"]["state"] == "retired"
    assert (
        c.execute(
            "SELECT count(*) AS n FROM braincore.legacy_native_authority WHERE native_id=%s",
            (native_id,),
        ).fetchone()["n"]
        == 1
    )


def test_artifact_retirement_preserved_and_source_conflict_rolls_back(
    typed_source: dict[str, Any],
) -> None:
    c, sid = typed_source["target"], typed_source["source_id"]
    import_artifacts(c, sid)
    result = mutate_artifact(
        c,
        principal(typed_source),
        object_id=typed_source["same_external"],
        fields={"original_path": "accepted/path"},
        action="retire",
        idempotency_key="artifact-retire",
    )
    native_id = result["native_id"]
    snapshot = c.execute(
        "SELECT row_to_json(a)::jsonb AS artifact,row_to_json(o)::jsonb AS mapping "
        "FROM braincore.artifacts a JOIN braincore.legacy_artifact_map o "
        "ON o.native_artifact_id=a.id WHERE a.id=%s",
        (native_id,),
    ).fetchone()
    import_artifacts(c, sid)
    assert (
        c.execute(
            "SELECT row_to_json(a)::jsonb AS artifact,row_to_json(o)::jsonb AS mapping "
            "FROM braincore.artifacts a JOIN braincore.legacy_artifact_map o "
            "ON o.native_artifact_id=a.id WHERE a.id=%s",
            (native_id,),
        ).fetchone()
        == snapshot
    )
    c.execute("UPDATE synthetic_legacy.artifact SET source_type='later-source-type'")
    recapture(typed_source)
    with pytest.raises(psycopg.errors.CheckViolation, match="source_native_authority_conflict"):
        import_artifacts(c, sid, changed_only=True)
    assert (
        c.execute(
            "SELECT row_to_json(a)::jsonb AS artifact,row_to_json(o)::jsonb AS mapping "
            "FROM braincore.artifacts a JOIN braincore.legacy_artifact_map o "
            "ON o.native_artifact_id=a.id WHERE a.id=%s",
            (native_id,),
        ).fetchone()
        == snapshot
    )


def test_worker_vector_only_change_remains_importable(typed_source: dict[str, Any]) -> None:
    c, sid = typed_source["target"], typed_source["source_id"]
    native_id = c.execute(
        "SELECT id FROM braincore.memories WHERE object_type='segment' AND legacy_external_id=%s",
        (typed_source["same_external"],),
    ).fetchone()["id"]
    vector = "[1," + ",".join(["0"] * 383) + "]"
    c.execute(
        "UPDATE braincore.memories SET embedding=%s::vector,embedding_state='ready',"
        "embedding_model='test-vector-only',embedding_preprocessing_revision='test',"
        "updated_at=now() WHERE id=%s",
        (vector, native_id),
    )
    import_objects(c, sid)
    assert (
        c.execute(
            "SELECT embedding::text AS vector FROM braincore.memories WHERE id=%s", (native_id,)
        ).fetchone()["vector"]
        == vector
    )
    assert not c.execute(
        "SELECT 1 FROM braincore.legacy_native_authority WHERE native_id=%s", (native_id,)
    ).fetchone()
    c.execute(
        "UPDATE synthetic_legacy.segment SET content='Changed source content' WHERE segment_id=%s",
        (typed_source["same_external"],),
    )
    recapture(typed_source)
    import_objects(c, sid, changed_only=True)
    assert c.execute(
        "SELECT content,embedding FROM braincore.memories WHERE id=%s", (native_id,)
    ).fetchone() == {"content": "Changed source content", "embedding": None}


def test_unsupported_snapshot_fails_closed(typed_source: dict[str, Any]) -> None:
    c = typed_source["target"]
    row = c.execute(
        "SELECT native_memory_id,imported_payload_hash FROM braincore.legacy_object_map LIMIT 1"
    ).fetchone()
    with c.transaction():
        c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        with pytest.raises(ValueError, match="requires READ COMMITTED"):
            guard_import_rows(
                c, "memories", [(row["native_memory_id"], row["imported_payload_hash"])]
            )


def test_waiting_import_sees_committed_metadata_only_native_authority(
    typed_source: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    c, sid = typed_source["target"], typed_source["source_id"]
    actor = principal(typed_source)
    c.execute(
        "UPDATE synthetic_legacy.segment SET content='Later source content' WHERE segment_id=%s",
        (typed_source["same_external"],),
    )
    recapture(typed_source)
    ready = Event()
    from braincore_memory import legacy_objects

    original = legacy_objects.guard_import_rows

    def observed(conn: Any, relation: str, rows: Any) -> set[Any]:
        ready.set()
        return original(conn, relation, rows)

    monkeypatch.setattr(legacy_objects, "guard_import_rows", observed)

    def importing() -> None:
        with psycopg.connect(typed_source["dsn"], autocommit=True, row_factory=dict_row) as db:
            db.execute("SET statement_timeout='5s'")
            import_objects(db, sid, changed_only=True)

    with ThreadPoolExecutor(max_workers=1) as pool:
        with c.transaction():
            result = mutate_legacy_object(
                c,
                actor,
                object_type="segment",
                object_id=typed_source["same_external"],
                fields={"line_start": 99},
                idempotency_key="metadata-only-concurrent",
            )
            accepted = memory_state(c, result["native_id"])
            future = pool.submit(importing)
            assert ready.wait(3)
            # Prove the competing connection actually waited on a PostgreSQL lock.
            blocker = c.execute(
                "SELECT 1 FROM pg_stat_activity WHERE datname=current_database() "
                "AND pid<>pg_backend_pid() AND wait_event_type='Lock' LIMIT 1"
            ).fetchone()
            for _ in range(30):
                if blocker:
                    break
                Event().wait(0.01)
                blocker = c.execute(
                    "SELECT 1 FROM pg_stat_activity WHERE datname=current_database() "
                    "AND pid<>pg_backend_pid() AND wait_event_type='Lock' LIMIT 1"
                ).fetchone()
            assert blocker and not future.done()
        with pytest.raises(psycopg.errors.CheckViolation, match="source_native_authority_conflict"):
            future.result(timeout=6)
    assert memory_state(c, result["native_id"]) == accepted


def test_mapped_provenance_retirement_survives_replay_and_blocks_source_change(
    typed_source: dict[str, Any],
) -> None:
    c, sid = typed_source["target"], typed_source["source_id"]
    external = uuid4()
    c.execute(
        "INSERT INTO synthetic_legacy.fact_evidence(fact_evidence_id,fact_id,segment_id,excerpt) "
        "VALUES (%s,%s,%s,'source quote')",
        (external, typed_source["same_external"], typed_source["same_external"]),
    )
    recapture(typed_source)
    import_provenance(c, sid)
    mutate_control(
        c,
        principal(typed_source),
        object_type="fact_evidence",
        object_id=external,
        fields={},
        action="retire",
        idempotency_key="retire-evidence",
    )
    accepted = c.execute(
        "SELECT row_to_json(e)::jsonb AS row FROM braincore.legacy_fact_evidence e "
        "WHERE attributes->>'fact_evidence_id'=%s",
        (str(external),),
    ).fetchone()
    import_provenance(c, sid)
    assert reconcile_provenance(c, sid)["semantic_per_key_reconciliation_gate"] == "PASS"
    assert (
        c.execute(
            "SELECT row_to_json(e)::jsonb AS row FROM braincore.legacy_fact_evidence e "
            "WHERE attributes->>'fact_evidence_id'=%s",
            (str(external),),
        ).fetchone()
        == accepted
    )
    c.execute(
        "UPDATE synthetic_legacy.fact_evidence SET excerpt='later source quote' "
        "WHERE fact_evidence_id=%s",
        (external,),
    )
    recapture(typed_source)
    with pytest.raises(psycopg.errors.CheckViolation, match="source_native_authority_conflict"):
        import_provenance(c, sid)
    assert (
        c.execute(
            "SELECT row_to_json(e)::jsonb AS row FROM braincore.legacy_fact_evidence e "
            "WHERE attributes->>'fact_evidence_id'=%s",
            (str(external),),
        ).fetchone()
        == accepted
    )


def test_retained_audit_backfill_and_runtime_cannot_erase_or_forge_authority(
    typed_source: dict[str, Any],
) -> None:
    c = typed_source["target"]
    actor = principal(typed_source)
    result = mutate_legacy_object(
        c,
        actor,
        object_type="segment",
        object_id=typed_source["same_external"],
        fields={"line_start": 5},
        idempotency_key="retained-accepted",
    )
    native_id = result["native_id"]
    c.execute("DELETE FROM braincore.legacy_native_authority WHERE native_id=%s", (native_id,))
    migration = (migration_root() / "029_native_import_authority.sql").read_text()
    start = migration.index("INSERT INTO braincore.legacy_native_authority\n  (relation_name")
    end = migration.index("CREATE FUNCTION braincore.source_import_allowed", start)
    c.execute(migration[start:end])
    assert c.execute(
        "SELECT 1 FROM braincore.legacy_native_authority WHERE native_id=%s", (native_id,)
    ).fetchone()
    role = "authority_runtime_" + uuid4().hex
    c.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
    try:
        c.execute(sql.SQL("GRANT USAGE ON SCHEMA braincore TO {}").format(sql.Identifier(role)))
        c.execute(
            sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA braincore TO {}").format(
                sql.Identifier(role)
            )
        )
        c.execute(sql.SQL("GRANT UPDATE ON braincore.memories TO {}").format(sql.Identifier(role)))
        c.execute(
            sql.SQL("GRANT INSERT,UPDATE,DELETE ON braincore.audit_events TO {}").format(
                sql.Identifier(role)
            )
        )
        with c.transaction(force_rollback=True):
            c.execute(
                "DELETE FROM braincore.legacy_native_authority WHERE native_id=%s", (native_id,)
            )
            c.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
            c.execute(
                "UPDATE braincore.memories SET content='accepted with runtime role' WHERE id=%s",
                (native_id,),
            )
            c.execute(
                "INSERT INTO braincore.audit_events"
                "(tenant_id,actor_key_id,action,target_type,target_id) "
                "VALUES (%s,%s,'memory.revise','memory',%s)",
                (actor.tenant_id, actor.key_id, native_id),
            )
            c.execute("DELETE FROM braincore.audit_events WHERE target_id=%s", (native_id,))
            assert c.execute(
                "SELECT 1 FROM braincore.legacy_native_authority WHERE native_id=%s", (native_id,)
            ).fetchone()
            for query in (
                "DELETE FROM braincore.legacy_native_authority",
                "UPDATE braincore.legacy_native_authority SET imported_payload_hash=NULL",
                "TRUNCATE braincore.legacy_native_authority",
                "INSERT INTO braincore.legacy_native_authority"
                "(relation_name,native_id,first_audit_id) "
                "VALUES ('memories',gen_random_uuid(),gen_random_uuid())",
                "SELECT braincore.record_native_import_authority()",
            ):
                with pytest.raises(psycopg.errors.InsufficientPrivilege), c.transaction():
                    c.execute(query)
    finally:
        c.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
        c.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def test_mapped_auxiliary_correction_preserves_refs_and_blocks_source_change(
    typed_source: dict[str, Any],
) -> None:
    c, sid = typed_source["target"], typed_source["source_id"]
    external = uuid4()
    c.execute(
        "INSERT INTO synthetic_legacy.publish_note(publish_id,memory_id,file_path) "
        "VALUES (%s,%s,'source-note.md')",
        (external, typed_source["same_external"]),
    )
    recapture(typed_source)
    import_auxiliary_objects(c, sid)
    result = mutate_control(
        c,
        principal(typed_source),
        object_type="publish_note",
        object_id=external,
        fields={"file_path": "accepted-note.md"},
        idempotency_key="accepted-publish",
    )
    native_id = result["native_id"]
    accepted = c.execute(
        "SELECT row_to_json(a)::jsonb AS row FROM braincore.legacy_auxiliary_objects a "
        "WHERE native_id=%s",
        (native_id,),
    ).fetchone()
    refs = c.execute(
        "SELECT * FROM braincore.legacy_auxiliary_refs WHERE source_id=%s "
        "AND source_table='publish_note' ORDER BY source_column",
        (sid,),
    ).fetchall()
    import_auxiliary_objects(c, sid)
    assert (
        c.execute(
            "SELECT row_to_json(a)::jsonb AS row FROM braincore.legacy_auxiliary_objects a "
            "WHERE native_id=%s",
            (native_id,),
        ).fetchone()
        == accepted
    )
    assert (
        c.execute(
            "SELECT * FROM braincore.legacy_auxiliary_refs WHERE source_id=%s "
            "AND source_table='publish_note' ORDER BY source_column",
            (sid,),
        ).fetchall()
        == refs
    )
    c.execute(
        "UPDATE synthetic_legacy.publish_note SET file_path='later-source.md' WHERE publish_id=%s",
        (external,),
    )
    recapture(typed_source)
    with pytest.raises(psycopg.errors.CheckViolation, match="source_native_authority_conflict"):
        import_auxiliary_objects(c, sid)
    assert (
        c.execute(
            "SELECT row_to_json(a)::jsonb AS row FROM braincore.legacy_auxiliary_objects a "
            "WHERE native_id=%s",
            (native_id,),
        ).fetchone()
        == accepted
    )


def test_direct_evidence_quote_preserved_after_native_correction(
    typed_source: dict[str, Any],
) -> None:
    c, sid = typed_source["target"], typed_source["source_id"]
    policy = {"format_version": 1, "source_id": str(sid), "approved_shares": []}
    import_artifacts(c, sid)
    import_direct_evidence(c, sid, policy)
    result = mutate_legacy_object(
        c,
        principal(typed_source),
        object_type="segment",
        object_id=typed_source["same_external"],
        fields={"content": "Accepted new content"},
        idempotency_key="accepted-evidence-parent",
    )
    native_id = result["native_id"]
    accepted = c.execute(
        "SELECT * FROM braincore.memory_evidence WHERE memory_id=%s ORDER BY artifact_id",
        (native_id,),
    ).fetchall()
    assert accepted and accepted[0]["quote"] == "Exact original segment"
    receipt = import_direct_evidence(c, sid, policy)
    assert receipt["native_preserved_objects"] == 1
    assert (
        c.execute(
            "SELECT * FROM braincore.memory_evidence WHERE memory_id=%s ORDER BY artifact_id",
            (native_id,),
        ).fetchall()
        == accepted
    )
    c.execute(
        "UPDATE synthetic_legacy.segment SET content='Later source evidence' WHERE segment_id=%s",
        (typed_source["same_external"],),
    )
    recapture(typed_source)
    with pytest.raises(psycopg.errors.CheckViolation, match="source_native_authority_conflict"):
        import_direct_evidence(c, sid, policy)
    assert (
        c.execute(
            "SELECT * FROM braincore.memory_evidence WHERE memory_id=%s ORDER BY artifact_id",
            (native_id,),
        ).fetchall()
        == accepted
    )
