import os
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4, uuid5

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

from braincore_memory.auth import Principal
from braincore_memory.config import get_settings
from braincore_memory.legacy_evidence import import_artifacts, import_direct_evidence, scope_hash
from braincore_memory.legacy_ledger import copy_source
from braincore_memory.legacy_objects import import_objects, reconcile_objects
from braincore_memory.lexical_projection import backfill_projection
from braincore_memory.migrate import migration_root
from braincore_memory.models import SearchRequest
from braincore_memory.service import search_memories


@pytest.fixture
def typed_source() -> Iterator[dict[str, Any]]:
    if os.getenv("BRAINCORE_E2E") != "1":
        pytest.skip("requires disposable integration databases")
    dsn = get_settings().database_url.get_secret_value()
    database = "braincore_native_test_" + uuid4().hex
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as admin:
        assert (
            admin.execute("SELECT current_database() AS name").fetchone()["name"]
            == "braincore_test"
        )
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        native_dsn = make_conninfo(dsn, dbname=database)
        try:
            with psycopg.connect(native_dsn, autocommit=True, row_factory=dict_row) as target:
                for path in sorted(migration_root().glob("*.sql")):
                    with target.transaction():
                        target.execute(path.read_text())
                target.execute("CREATE SCHEMA synthetic_legacy")
                definitions = {
                    "entity": "canonical_name text",
                    "fact": "predicate text,object_value jsonb,current_status text",
                    "memory": (
                        "title text,narrative text,lifecycle_state text,governance_status text"
                    ),
                    "segment": "content text,artifact_id uuid",
                    "episode": "title text,summary text,primary_artifact_id uuid",
                    "artifact": "source_type text",
                }
                for table, definition in definitions.items():
                    target.execute(
                        sql.SQL("""CREATE TABLE {} ({} uuid PRIMARY KEY,tenant text,
                        scope_path text,valid_from timestamptz,valid_to timestamptz,
                        created_at timestamptz,fts tsvector,{})""").format(
                            sql.Identifier("synthetic_legacy", table),
                            sql.Identifier(table + "_id"),
                            sql.SQL(definition),
                        )
                    )
                same_external = uuid4()
                for table in definitions:
                    target.execute(
                        sql.SQL(
                            "INSERT INTO {} ({},tenant,scope_path) VALUES (%s,'alpha',%s)"
                        ).format(
                            sql.Identifier("synthetic_legacy", table), sql.Identifier(table + "_id")
                        ),
                        (same_external, "project/literal_%/nested"),
                    )
                target.execute(
                    "UPDATE synthetic_legacy.entity SET canonical_name='Original entity'"
                )
                target.execute("""UPDATE synthetic_legacy.fact SET predicate='original assertion',
                                  object_value='null',current_status='active'""")
                target.execute("""UPDATE synthetic_legacy.memory SET title='Preserved memory',
                                  narrative='Independent narrative',lifecycle_state='published',
                                  governance_status='validated'""")
                target.execute(
                    "UPDATE synthetic_legacy.segment SET content='Exact original segment'"
                )
                target.execute(
                    "UPDATE synthetic_legacy.episode SET title='Episode',summary='Original summary'"
                )
                target.execute(
                    """INSERT INTO synthetic_legacy.fact
                    (fact_id,tenant,scope_path,valid_from,valid_to,predicate,current_status)
                    VALUES (%s,'alpha','project/literal_%%/nested','2000-01-01','2020-01-01',
                            'expired assertion','active'),
                           (%s,'beta','project/literal_%%/nested',NULL,NULL,
                            'other tenant','active')""",
                    (uuid4(), uuid4()),
                )
                target.execute(
                    """INSERT INTO synthetic_legacy.segment
                    (segment_id,tenant,scope_path,content)
                    VALUES (%s,'alpha','project/literal_XX/nested','Wildcard negative')""",
                    (uuid4(),),
                )
                target.execute(
                    "UPDATE synthetic_legacy.segment SET artifact_id=%s", (same_external,)
                )
                target.execute(
                    "UPDATE synthetic_legacy.episode SET primary_artifact_id=%s", (same_external,)
                )
                target.execute("UPDATE synthetic_legacy.artifact SET source_type='synthetic'")
                target.execute("""CREATE TABLE synthetic_legacy.fact_evidence (
                    fact_evidence_id uuid PRIMARY KEY,fact_id uuid,segment_id uuid,
                    excerpt text,weight numeric,created_at timestamptz)""")
                target.execute("""CREATE TABLE synthetic_legacy.memory_support (
                    support_id uuid PRIMARY KEY,memory_id uuid,fact_id uuid,episode_id uuid,
                    support_type text,notes text,created_at timestamptz)""")
                target.execute("""CREATE TABLE synthetic_legacy.memory_revision (
                    revision_id uuid PRIMARY KEY,memory_id uuid,tenant text,
                    revision_type text,old_narrative text,new_narrative text,
                    change_reason text,created_at timestamptz)""")
                target.execute("""CREATE TABLE synthetic_legacy.memory_edge (
                    edge_id uuid PRIMARY KEY,tenant text,scope_path text,
                    source_type text,source_id uuid,target_type text,target_id uuid,
                    evidence_segment_id uuid)""")
                target.execute("""CREATE TABLE synthetic_legacy.publish_note (
                    publish_id uuid PRIMARY KEY,memory_id uuid,file_path text)""")
                target.execute("""CREATE TABLE synthetic_legacy.reflection_class (
                    class_key text PRIMARY KEY,parent_class_key text
                      REFERENCES synthetic_legacy.reflection_class(class_key),
                    description text)""")
                source_id = uuid4()
                with psycopg.connect(native_dsn, autocommit=True, row_factory=dict_row) as source:
                    copy_source(source, target, source_id, "synthetic_legacy", batch_size=2)
                result = import_objects(target, source_id, batch_size=1)
                backfill_projection(native_dsn, batch_size=2)
                yield {
                    "target": target,
                    "dsn": native_dsn,
                    "source_id": source_id,
                    "same_external": same_external,
                    "result": result,
                }
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))


def test_native_typed_identity_unknown_validity_and_retry(typed_source: dict[str, Any]) -> None:
    target, source_id = typed_source["target"], typed_source["source_id"]
    assert typed_source["result"]["row_count"] == 8
    assert typed_source["result"]["embedding_coverage_gate"] == "NOT RUN"
    rows = target.execute(
        """SELECT id,object_type,legacy_record_key::text AS key,valid_from,
                                    embedding,embedding_model,legacy_external_id
                             FROM braincore.memories WHERE legacy_source_id=%s""",
        (source_id,),
    ).fetchall()
    assert len({r["id"] for r in rows}) == 8
    for row in rows:
        assert row["id"] == uuid5(source_id, row["object_type"] + ":" + row["key"])
        assert row["embedding"] is None and row["embedding_model"] is None
    assert any(r["valid_from"] is None for r in rows)
    import_objects(target, source_id, batch_size=2)
    assert target.execute("SELECT count(*) AS n FROM braincore.memories").fetchone()["n"] == 8
    assert (
        target.execute("SELECT count(*) AS n FROM braincore.legacy_object_map").fetchone()["n"] == 8
    )
    original = target.execute(
        "SELECT content FROM braincore.memories WHERE object_type='fact' AND legacy_external_id=%s",
        (typed_source["same_external"],),
    ).fetchone()
    assert original["content"] == "original assertion null"
    # Unknown governance must not reactivate a source record during a retry.
    target.execute("UPDATE synthetic_legacy.memory SET governance_status=NULL")
    with psycopg.connect(typed_source["dsn"], autocommit=True, row_factory=dict_row) as source:
        copy_source(source, target, source_id, "synthetic_legacy")
    import_objects(target, source_id)
    unknown = target.execute(
        "SELECT state,metadata FROM braincore.memories WHERE object_type='memory'"
    ).fetchone()
    assert unknown["state"] == "suppressed"
    assert unknown["metadata"]["legacy"]["created_at_origin"] == "migration"


async def test_native_filters_precede_limits_and_literal_scope_prefixes(
    typed_source: dict[str, Any],
) -> None:
    target = typed_source["target"]
    tenant = target.execute("SELECT id FROM braincore.tenants WHERE name='alpha'").fetchone()["id"]
    principal = Principal(uuid4(), tenant, frozenset({"read", "admin"}))

    @asynccontextmanager
    async def connect() -> AsyncIterator[Any]:
        async with await psycopg.AsyncConnection.connect(
            typed_source["dsn"], row_factory=dict_row
        ) as conn:
            yield conn

    current = await search_memories(
        connect, principal, SearchRequest(object_type="fact", limit=20), None, get_settings()
    )
    assert len(current.payload["results"]) == 1
    past = await search_memories(
        connect,
        principal,
        SearchRequest(object_type="fact", as_of=datetime(2010, 1, 1, tzinfo=UTC)),
        None,
        get_settings(),
    )
    assert len(past.payload["results"]) == 2
    segments = await search_memories(
        connect,
        principal,
        SearchRequest(object_type="segment", legacy_scope_prefix="project/literal_%/", limit=20),
        None,
        get_settings(),
    )
    assert [r["content"] for r in segments.payload["results"]] == ["Exact original segment"]
    negative = await search_memories(
        connect,
        principal,
        SearchRequest(legacy_scope_prefix="impossible-scope/", limit=20),
        None,
        get_settings(),
    )
    assert negative.payload["results"] == []
    unfiltered = await search_memories(
        connect, principal, SearchRequest(limit=20), None, get_settings()
    )
    assert all(r["object_type"] != "entity" for r in unfiltered.payload["results"])


def test_native_evidence_requires_explicit_resource_scope_policy_and_never_regrants(
    typed_source: dict[str, Any],
) -> None:
    target, source_id = typed_source["target"], typed_source["source_id"]
    target.execute("UPDATE synthetic_legacy.artifact SET tenant='beta'")
    with psycopg.connect(typed_source["dsn"], autocommit=True, row_factory=dict_row) as source:
        copy_source(source, target, source_id, "synthetic_legacy")
    assert import_artifacts(target, source_id) == 1
    empty_policy = {"format_version": 1, "source_id": str(source_id), "approved_shares": []}
    with pytest.raises(ValueError, match="approval"):
        import_direct_evidence(target, source_id, empty_policy)
    scopes = target.execute(
        "SELECT DISTINCT scope FROM braincore.memories WHERE object_type IN ('segment','episode')"
    ).fetchall()
    policy = {
        **empty_policy,
        "approved_shares": [
            {
                "artifact_external_id": str(typed_source["same_external"]),
                "grantee_original_tenant": "alpha",
                "reason": "Verified explicit test sharing",
                "scope_hashes": [scope_hash(r["scope"]) for r in scopes],
            }
        ],
    }
    result = import_direct_evidence(target, source_id, policy, batch_size=1)
    assert result["linked_objects"] == 3 and result["cross_owner_links"] == 3
    import_direct_evidence(target, source_id, policy, batch_size=2)
    assert (
        target.execute("SELECT count(*) AS n FROM braincore.memory_evidence").fetchone()["n"] == 3
    )
    assert (
        target.execute("SELECT count(*) AS n FROM braincore.artifact_access_grants").fetchone()["n"]
        == 2
    )
    assert (
        target.execute(
            "SELECT t.name FROM braincore.artifacts a JOIN braincore.tenants t ON t.id=a.tenant_id"
        ).fetchone()["name"]
        == "beta"
    )
    grants = target.execute("SELECT id FROM braincore.artifact_access_grants").fetchall()
    target.execute(
        "UPDATE braincore.artifact_access_grants SET revoked_at=now() WHERE id=%s",
        (grants[0]["id"],),
    )
    with pytest.raises(ValueError, match="revoked"):
        import_direct_evidence(target, source_id, policy)
    assert (
        target.execute(
            "SELECT revoked_at FROM braincore.artifact_access_grants WHERE id=%s",
            (grants[0]["id"],),
        ).fetchone()["revoked_at"]
        is not None
    )


def test_native_provenance_retains_edge_identity_revision_and_owner_guards(
    typed_source: dict[str, Any],
) -> None:
    from braincore_memory.legacy_provenance import import_provenance, reconcile_provenance

    target, source_id = typed_source["target"], typed_source["source_id"]
    external = typed_source["same_external"]
    evidence_id, support_one, support_two, revision_id = (uuid4() for _ in range(4))
    target.execute(
        """INSERT INTO synthetic_legacy.fact_evidence
      VALUES (%s,%s,%s,'Original precision',0.123456789012345678901,NULL)""",
        (evidence_id, external, external),
    )
    for support in (support_one, support_two):
        target.execute(
            """INSERT INTO synthetic_legacy.memory_support
          VALUES (%s,%s,%s,NULL,'supporting','Repeated source edge',NULL)""",
            (support, external, external),
        )
    target.execute(
        """INSERT INTO synthetic_legacy.memory_revision
      VALUES (%s,%s,'alpha','correction','Before','After','Original correction','2010-01-01')""",
        (revision_id, external),
    )
    with psycopg.connect(typed_source["dsn"], autocommit=True, row_factory=dict_row) as source:
        copy_source(source, target, source_id, "synthetic_legacy")
    result = import_provenance(target, source_id, batch_size=1)
    assert result["objects"] == {"fact_evidence": 1, "memory_support": 2, "memory_revision": 1}
    row = target.execute("""SELECT e.native_fact_id,e.native_segment_id,v.payload::text AS payload
      FROM braincore.legacy_fact_evidence e JOIN braincore.legacy_record_versions v
      USING(source_id,source_table,record_key)
      WHERE v.payload_hash=e.imported_payload_hash""").fetchone()
    assert row["native_fact_id"] != row["native_segment_id"]
    assert "0.123456789012345678901" in row["payload"]
    assert "Original precision" in row["payload"]
    original_xmins = target.execute(
        "SELECT native_id,xmin::text FROM braincore.legacy_memory_support ORDER BY native_id"
    ).fetchall()
    assert import_provenance(target, source_id, batch_size=2)["processed_rows"] == 0
    assert (
        target.execute(
            "SELECT native_id,xmin::text FROM braincore.legacy_memory_support ORDER BY native_id"
        ).fetchall()
        == original_xmins
    )
    assert (
        reconcile_provenance(target, source_id, batch_size=1)[
            "semantic_per_key_reconciliation_gate"
        ]
        == "PASS"
    )
    target.execute("UPDATE braincore.legacy_memory_support SET native_fact_id=NULL")
    assert (
        reconcile_provenance(target, source_id, batch_size=1)["tables"]["memory_support"][
            "mismatched"
        ]
        == 2
    )
    target.execute(
        "UPDATE braincore.legacy_memory_support SET native_fact_id=%s", (row["native_fact_id"],)
    )
    assert (
        target.execute("SELECT count(*) AS n FROM braincore.legacy_memory_support").fetchone()["n"]
        == 2
    )
    # A changed source relationship cannot quietly attach another tenant's object.
    beta = target.execute(
        "SELECT fact_id FROM synthetic_legacy.fact WHERE tenant='beta'"
    ).fetchone()["fact_id"]
    target.execute("UPDATE synthetic_legacy.fact_evidence SET fact_id=%s", (beta,))
    with psycopg.connect(typed_source["dsn"], autocommit=True, row_factory=dict_row) as source:
        copy_source(source, target, source_id, "synthetic_legacy")
    with pytest.raises(ValueError, match="ownership"):
        import_provenance(target, source_id)
    unchanged = target.execute(
        "SELECT native_fact_id FROM braincore.legacy_fact_evidence"
    ).fetchone()
    assert unchanged["native_fact_id"] == row["native_fact_id"]


async def test_original_provenance_visibility_and_shared_artifact_revocation(
    typed_source: dict[str, Any],
) -> None:
    from braincore_memory.legacy_provenance import import_provenance, read_provenance

    target, source_id = typed_source["target"], typed_source["source_id"]
    external = typed_source["same_external"]
    target.execute("UPDATE synthetic_legacy.artifact SET tenant='beta'")
    target.execute(
        "INSERT INTO synthetic_legacy.fact_evidence VALUES (%s,%s,%s,'Protected quote',1,NULL)",
        (uuid4(), external, external),
    )
    with psycopg.connect(typed_source["dsn"], autocommit=True, row_factory=dict_row) as source:
        copy_source(source, target, source_id, "synthetic_legacy")
    import_artifacts(target, source_id)
    scopes = target.execute(
        "SELECT DISTINCT scope FROM braincore.memories WHERE object_type IN ('segment','episode')"
    ).fetchall()
    policy = {
        "format_version": 1,
        "source_id": str(source_id),
        "approved_shares": [
            {
                "artifact_external_id": str(external),
                "grantee_original_tenant": "alpha",
                "reason": "Explicit test approval",
                "scope_hashes": [scope_hash(row["scope"]) for row in scopes],
            }
        ],
    }
    import_direct_evidence(target, source_id, policy)
    import_provenance(target, source_id)
    tenant = target.execute("SELECT id FROM braincore.tenants WHERE name='alpha'").fetchone()["id"]
    fact = target.execute(
        "SELECT id FROM braincore.memories WHERE object_type='fact' AND legacy_external_id=%s",
        (external,),
    ).fetchone()["id"]
    principal = Principal(uuid4(), tenant, frozenset({"read", "admin"}))
    async with await psycopg.AsyncConnection.connect(
        typed_source["dsn"], row_factory=dict_row
    ) as conn:
        visible = await read_provenance(conn, principal, fact)
        assert [row["excerpt"] for row in visible["evidence"]] == ["Protected quote"]
        target.execute("UPDATE braincore.artifact_access_grants SET revoked_at=now()")
        revoked = await read_provenance(conn, principal, fact)
        assert revoked["evidence"] == []
        # Another tenant and a key outside the root scope cannot read the source quote.
        beta = target.execute("SELECT id FROM braincore.tenants WHERE name='beta'").fetchone()["id"]
        from fastapi import HTTPException

        with pytest.raises(HTTPException):
            await read_provenance(
                conn, Principal(uuid4(), beta, frozenset({"read", "admin"})), fact
            )
        with pytest.raises(HTTPException):
            await read_provenance(
                conn, Principal(uuid4(), tenant, frozenset({"read"}), {"project_id": "other"}), fact
            )


def test_empty_source_text_is_retained_without_impossible_embedding_jobs(
    typed_source: dict[str, Any],
) -> None:
    target, source_id = typed_source["target"], typed_source["source_id"]
    external = typed_source["same_external"]
    target.execute(
        "UPDATE synthetic_legacy.segment SET content='' WHERE segment_id=%s", (external,)
    )
    with psycopg.connect(typed_source["dsn"], autocommit=True, row_factory=dict_row) as source:
        copy_source(source, target, source_id, "synthetic_legacy")
    import_objects(target, source_id)
    empty = target.execute(
        """SELECT content,embedding_state,embedding,embedding_model,
      embedding_preprocessing_revision FROM braincore.memories
      WHERE object_type='segment' AND legacy_external_id=%s""",
        (external,),
    ).fetchone()
    assert empty["content"] == "" and empty["embedding_state"] == "not_applicable"
    assert empty["embedding"] is None and empty["embedding_model"] is None
    assert empty["embedding_preprocessing_revision"] is None
    # Restored meaningful text becomes pending again instead of retaining an invalid exemption.
    target.execute(
        "UPDATE synthetic_legacy.segment SET content='Restored original text' WHERE segment_id=%s",
        (external,),
    )
    with psycopg.connect(typed_source["dsn"], autocommit=True, row_factory=dict_row) as source:
        copy_source(source, target, source_id, "synthetic_legacy")
    import_objects(target, source_id)
    restored = target.execute(
        """SELECT content,embedding_state FROM braincore.memories
           WHERE object_type='segment' AND legacy_external_id=%s""",
        (external,),
    ).fetchone()
    assert (
        restored["content"] == "Restored original text" and restored["embedding_state"] == "pending"
    )


def test_changed_only_resume_preserves_mapping_and_applies_captured_content(
    typed_source: dict[str, Any],
) -> None:
    target, source_id = typed_source["target"], typed_source["source_id"]
    unchanged = import_objects(target, source_id, batch_size=1, changed_only=True)
    assert unchanged["row_count"] == 8
    assert unchanged["processed_rows"] == 0
    target.execute("UPDATE synthetic_legacy.segment SET content='Changed source content'")
    with psycopg.connect(typed_source["dsn"], autocommit=True, row_factory=dict_row) as source:
        copy_source(source, target, source_id, "synthetic_legacy")
    changed = import_objects(target, source_id, batch_size=1, changed_only=True)
    assert changed["row_count"] == 8
    assert changed["processed_rows"] == 2
    assert (
        target.execute(
            "SELECT count(*) AS n FROM braincore.memories WHERE object_type='segment' "
            "AND content='Changed source content'"
        ).fetchone()["n"]
        == 2
    )


async def test_fact_root_grants_require_exact_scopes_and_do_not_renew_expiry(
    typed_source: dict[str, Any],
) -> None:
    from braincore_memory.legacy_provenance import (
        import_fact_evidence_grants,
        import_provenance,
        read_provenance,
    )

    target, source_id = typed_source["target"], typed_source["source_id"]
    external = typed_source["same_external"]
    target.execute("UPDATE synthetic_legacy.artifact SET tenant='beta'")
    target.execute(
        "UPDATE synthetic_legacy.fact SET scope_path='fact/root' WHERE fact_id=%s", (external,)
    )
    target.execute(
        "INSERT INTO synthetic_legacy.fact_evidence VALUES (%s,%s,%s,'Root quote',1,NULL)",
        (uuid4(), external, external),
    )
    with psycopg.connect(typed_source["dsn"], autocommit=True, row_factory=dict_row) as source:
        copy_source(source, target, source_id, "synthetic_legacy")
    import_objects(target, source_id, changed_only=True)
    import_artifacts(target, source_id)
    scopes = target.execute(
        "SELECT DISTINCT scope FROM braincore.memories WHERE object_type IN ('segment','episode')"
    ).fetchall()
    policy = {
        "format_version": 1,
        "source_id": str(source_id),
        "approved_shares": [
            {
                "artifact_external_id": str(external),
                "grantee_original_tenant": "alpha",
                "reason": "Verified original fact root",
                "scope_hashes": [scope_hash(r["scope"]) for r in scopes],
            }
        ],
    }
    import_direct_evidence(target, source_id, policy)
    import_provenance(target, source_id)
    with pytest.raises(ValueError, match="root/scope"):
        import_fact_evidence_grants(target, source_id, policy)
    fact = target.execute(
        "SELECT id,scope,tenant_id FROM braincore.memories WHERE object_type='fact' "
        "AND legacy_external_id=%s",
        (external,),
    ).fetchone()
    principal = Principal(uuid4(), fact["tenant_id"], frozenset({"read", "admin"}))
    async with await psycopg.AsyncConnection.connect(
        typed_source["dsn"], row_factory=dict_row
    ) as conn:
        assert (await read_provenance(conn, principal, fact["id"]))["evidence"] == []
        policy["approved_shares"][0]["scope_hashes"].append(scope_hash(fact["scope"]))
        result = import_fact_evidence_grants(target, source_id, policy, batch_size=1)
        assert result["cross_owner_fact_links"] == 1
        assert result["distinct_exact_scope_grants"] == 1
        assert len((await read_provenance(conn, principal, fact["id"]))["evidence"]) == 1
        audits = target.execute("SELECT count(*) AS n FROM braincore.audit_events").fetchone()["n"]
        import_fact_evidence_grants(target, source_id, policy)
        assert (
            target.execute("SELECT count(*) AS n FROM braincore.audit_events").fetchone()["n"]
            == audits
        )
        # Seed an already expired immutable grant in this disposable synthetic database.
        expired = target.execute(
            "DELETE FROM braincore.artifact_access_grants WHERE scope=%s RETURNING *",
            (psycopg.types.json.Jsonb(fact["scope"]),),
        ).fetchone()
        target.execute(
            "INSERT INTO braincore.artifact_access_grants "
            "(id,artifact_id,owner_tenant_id,grantee_tenant_id,scope,reason,created_at,expires_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,now()-interval '2 seconds',now()-interval '1 second')",
            (
                expired["id"],
                expired["artifact_id"],
                expired["owner_tenant_id"],
                expired["grantee_tenant_id"],
                psycopg.types.json.Jsonb(expired["scope"]),
                expired["reason"],
            ),
        )
        assert (await read_provenance(conn, principal, fact["id"]))["evidence"] == []
        with pytest.raises(ValueError, match="expired"):
            import_fact_evidence_grants(target, source_id, policy)


def test_native_reconciliation_detects_content_and_owner_drift(
    typed_source: dict[str, Any],
) -> None:
    target, source_id = typed_source["target"], typed_source["source_id"]
    result = reconcile_objects(target, source_id, batch_size=1)
    assert result["typed_semantic_per_key_reconciliation_gate"] == "PASS"
    assert result["row_count"] == 8
    target.execute(
        "UPDATE braincore.memories SET content='Unexpected drift' WHERE object_type='segment'"
    )
    result = reconcile_objects(target, source_id, batch_size=2)
    assert result["typed_semantic_per_key_reconciliation_gate"] == "FAIL"
    assert result["tables"]["segment"]["mismatched"] == 2


def test_auxiliary_constraints_owners_precise_payload_and_current_correction(
    typed_source: dict[str, Any],
) -> None:
    from braincore_memory.legacy_provenance import import_auxiliary_objects

    target, source_id = typed_source["target"], typed_source["source_id"]
    external = typed_source["same_external"]
    edge, publish = uuid4(), uuid4()
    target.execute(
        """INSERT INTO synthetic_legacy.memory_edge
       VALUES (%s,'alpha','edge/root','fact',%s,'memory',%s,%s)""",
        (edge, external, external, external),
    )
    target.execute(
        "INSERT INTO synthetic_legacy.publish_note VALUES (%s,%s,'original.md')",
        (publish, external),
    )
    target.execute("""INSERT INTO synthetic_legacy.reflection_class
        VALUES ('root',NULL,'Original'),('child','root','Child')""")
    with psycopg.connect(typed_source["dsn"], autocommit=True, row_factory=dict_row) as source:
        copy_source(source, target, source_id, "synthetic_legacy")
    result = import_auxiliary_objects(target, source_id, batch_size=1)
    assert result["row_count"] == 4 and result["constrained_references"] == 5
    owned = target.execute("""SELECT a.source_table,t.name,a.scope FROM
        braincore.legacy_auxiliary_objects a LEFT JOIN braincore.tenants t ON t.id=a.tenant_id
        ORDER BY source_table,record_key""").fetchall()
    assert [r["name"] for r in owned] == ["alpha", "alpha", None, None]
    target.execute("""UPDATE braincore.legacy_auxiliary_objects
        SET payload=jsonb_set(payload,'{file_path}','"corrected.md"')
        WHERE source_table='publish_note'""")
    import_auxiliary_objects(target, source_id)
    # Unchanged historical input cannot erase a current canonical correction.
    assert (
        target.execute("""SELECT payload->>'file_path' AS path
       FROM braincore.legacy_auxiliary_objects WHERE source_table='publish_note'""").fetchone()[
            "path"
        ]
        == "corrected.md"
    )
    # The BEFORE trigger rejects a missing endpoint before the FK is evaluated.
    with pytest.raises(psycopg.errors.RaiseException, match="auxiliary endpoint type or owner"):
        target.execute(
            """UPDATE braincore.legacy_auxiliary_refs SET native_memory_id=%s
          WHERE source_table='publish_note'""",
            (uuid4(),),
        )
    beta = target.execute(
        "SELECT fact_id FROM synthetic_legacy.fact WHERE tenant='beta'"
    ).fetchone()["fact_id"]
    target.execute("UPDATE synthetic_legacy.memory_edge SET source_id=%s", (beta,))
    with psycopg.connect(typed_source["dsn"], autocommit=True, row_factory=dict_row) as source:
        copy_source(source, target, source_id, "synthetic_legacy")
    with pytest.raises(ValueError, match="source owner"):
        import_auxiliary_objects(target, source_id)


def _fact_heads(typed_source: dict[str, Any]) -> list[dict[str, Any]]:
    return (
        typed_source["target"]
        .execute(
            """SELECT h.record_key::text AS key,v.payload,o.native_memory_id
           FROM braincore.legacy_record_heads h
           JOIN braincore.legacy_record_versions v USING
             (source_id,source_table,record_key,payload_hash)
           JOIN braincore.legacy_object_map o USING(source_id,source_table,record_key)
           WHERE h.source_id=%s AND h.source_table='fact' ORDER BY h.record_key""",
            (typed_source["source_id"],),
        )
        .fetchall()
    )


def _stage_fact_changes(
    typed_source: dict[str, Any], changes: list[tuple[str, dict[str, Any], bool]]
) -> None:
    import json

    from braincore_memory.legacy_ledger import stage_records

    target = typed_source["target"]
    with target.transaction():
        stage_records(
            target,
            typed_source["source_id"],
            [
                ("fact", key, json.dumps(payload), deleted, 1000, True)
                for key, payload, deleted in changes
            ],
        )


def test_finite_import_mixed_pages_preserve_vectors_and_ignore_unrelated_heads(
    typed_source: dict[str, Any],
) -> None:
    import json

    from braincore_memory.legacy_ledger import stage_records

    target, source_id = typed_source["target"], typed_source["source_id"]
    original = _fact_heads(typed_source)
    vector = "[" + ",".join(["1"] + ["0"] * 383) + "]"
    for row in original:
        target.execute(
            """UPDATE braincore.memories SET embedding=%s::public.vector,
               embedding_state='ready',embedding_model='verified-test-model',
               embedding_preprocessing_revision='test-v1' WHERE id=%s""",
            (vector, row["native_memory_id"]),
        )
    unchanged = original[0]
    preserved = original[1]
    tombstone = original[2]
    original_xmin = target.execute(
        "SELECT xmin::text AS version FROM braincore.memories WHERE id=%s",
        (unchanged["native_memory_id"],),
    ).fetchone()["version"]
    unrelated = target.execute(
        "SELECT id,xmin::text AS version,content FROM braincore.memories "
        "WHERE object_type='segment' ORDER BY id LIMIT 1"
    ).fetchone()
    changes = [(unchanged["key"], unchanged["payload"], False)]
    changes.append((preserved["key"], {**preserved["payload"], "priority": 7}, False))
    changes.append((tombstone["key"], {**tombstone["payload"], "predicate": "removed"}, True))
    for _ in range(25):
        external = str(uuid4())
        changes.append(
            (
                json.dumps({"fact_id": external}),
                {
                    "fact_id": external,
                    "tenant": "new-owner",
                    "scope_path": "finite/scope",
                    "predicate": "precise new fact",
                    "object_value": {"integer": "12345678901234567890"},
                    "current_status": "active",
                    "created_at": "2001-01-01T00:00:00Z",
                    "updated_at": "2002-01-01T00:00:00Z",
                    "valid_from": "2000-01-01T00:00:00Z",
                    "valid_to": None,
                },
                False,
            )
        )
    _stage_fact_changes(typed_source, changes)
    # An unrelated captured record has invalid ownership. Finite mapping must
    # neither rediscover it during tenant preparation nor claim its accounting.
    segment = target.execute(
        """SELECT h.record_key::text AS key,v.payload FROM braincore.legacy_record_heads h
           JOIN braincore.legacy_record_versions v USING
             (source_id,source_table,record_key,payload_hash)
           WHERE h.source_id=%s AND h.source_table='segment' LIMIT 1""",
        (source_id,),
    ).fetchone()
    with target.transaction():
        stage_records(
            target,
            source_id,
            [
                (
                    "segment",
                    segment["key"],
                    json.dumps({**segment["payload"], "tenant": "", "content": "unmapped"}),
                    False,
                    1000,
                    True,
                )
            ],
        )
    report = import_objects(
        target, source_id, batch_size=7, changed_keys={"fact": [key for key, _, _ in changes]}
    )
    assert report["typed_object_gate"] == "PASS"
    assert report["accounting_scope"] == "selected_keys"
    assert report["objects"] == {"fact": 28}
    assert report["processed_rows"] == 28
    assert (
        target.execute(
            "SELECT xmin::text AS version FROM braincore.memories WHERE id=%s",
            (unchanged["native_memory_id"],),
        ).fetchone()["version"]
        == original_xmin
    )
    retained = target.execute(
        """SELECT embedding::text AS vector,embedding_state,embedding_model,
                  embedding_preprocessing_revision FROM braincore.memories WHERE id=%s""",
        (preserved["native_memory_id"],),
    ).fetchone()
    assert retained == {
        "vector": vector,
        "embedding_state": "ready",
        "embedding_model": "verified-test-model",
        "embedding_preprocessing_revision": "test-v1",
    }
    retired = target.execute(
        "SELECT state,metadata,embedding,embedding_state FROM braincore.memories WHERE id=%s",
        (tombstone["native_memory_id"],),
    ).fetchone()
    assert retired["state"] == "retired" and retired["metadata"]["legacy"]["source_deleted"]
    assert retired["embedding"] is None and retired["embedding_state"] == "pending"
    assert (
        target.execute(
            "SELECT id,xmin::text AS version,content FROM braincore.memories WHERE id=%s",
            (unrelated["id"],),
        ).fetchone()
        == unrelated
    )
    fresh = target.execute(
        """SELECT m.created_at,m.updated_at,m.valid_from,m.content FROM braincore.memories m
           JOIN braincore.tenants t ON t.id=m.tenant_id WHERE t.name='new-owner' LIMIT 1"""
    ).fetchone()
    assert fresh["created_at"] == datetime(2001, 1, 1, tzinfo=UTC)
    assert fresh["updated_at"] == datetime(2002, 1, 1, tzinfo=UTC)
    assert fresh["valid_from"] == datetime(2000, 1, 1, tzinfo=UTC)
    assert "12345678901234567890" in fresh["content"]
    versions = target.execute(
        "SELECT id,xmin::text AS version FROM braincore.memories ORDER BY id"
    ).fetchall()
    retry = import_objects(
        target,
        source_id,
        batch_size=7,
        changed_only=True,
        changed_keys={"fact": [key for key, _, _ in changes]},
    )
    assert retry["processed_rows"] == 0 and retry["row_count"] == 28
    assert (
        target.execute(
            "SELECT id,xmin::text AS version FROM braincore.memories ORDER BY id"
        ).fetchall()
        == versions
    )


def test_finite_conflict_rolls_back_page_then_resumes_committed_pages(
    typed_source: dict[str, Any],
) -> None:
    from psycopg.types.json import Jsonb

    target, source_id = typed_source["target"], typed_source["source_id"]
    original = _fact_heads(typed_source)
    _stage_fact_changes(
        typed_source,
        [
            (row["key"], {**row["payload"], "predicate": "changed " + str(index)}, False)
            for index, row in enumerate(original)
        ],
    )
    conflict = original[1]
    target.execute(
        "UPDATE braincore.memories SET legacy_record_key=%s WHERE id=%s",
        (Jsonb({"fact_id": str(uuid4())}), conflict["native_memory_id"]),
    )
    keys = {"fact": [row["key"] for row in original]}
    with pytest.raises(ValueError, match="identity or ownership conflict"):
        import_objects(target, source_id, batch_size=1, changed_only=True, changed_keys=keys)
    # Page one committed, while the conflicting page and later page remain unmapped.
    assert (
        target.execute(
            "SELECT content FROM braincore.memories WHERE id=%s", (original[0]["native_memory_id"],)
        )
        .fetchone()["content"]
        .startswith("changed 0")
    )
    assert (
        not target.execute(
            "SELECT content FROM braincore.memories WHERE id=%s", (conflict["native_memory_id"],)
        )
        .fetchone()["content"]
        .startswith("changed 1")
    )
    target.execute(
        "UPDATE braincore.memories SET legacy_record_key=%s::jsonb WHERE id=%s",
        (conflict["key"], conflict["native_memory_id"]),
    )
    report = import_objects(target, source_id, batch_size=1, changed_only=True, changed_keys=keys)
    assert report["processed_rows"] == 2 and report["row_count"] == 3
    assert (
        reconcile_objects(target, source_id, batch_size=2)[
            "typed_semantic_per_key_reconciliation_gate"
        ]
        == "PASS"
    )


def test_finite_keys_require_retained_heads_and_preserve_caller_temp_relations(
    typed_source: dict[str, Any],
) -> None:
    import json

    target, source_id = typed_source["target"], typed_source["source_id"]
    versions = target.execute(
        "SELECT id,xmin::text AS version FROM braincore.memories ORDER BY id"
    ).fetchall()
    with pytest.raises(ValueError, match="no retained head"):
        import_objects(
            target, source_id, changed_keys={"fact": [json.dumps({"fact_id": str(uuid4())})]}
        )
    with pytest.raises(ValueError, match="source tables"):
        import_objects(target, source_id, changed_keys={"unreviewed": []})
    with pytest.raises(ValueError, match="JSON object"):
        import_objects(target, source_id, changed_keys={"fact": ["[]"]})
    for reserved in (
        "braincore_import_keys",
        "braincore_import_heads",
        "braincore_import_batch",
        "braincore_import_requested",
    ):
        target.execute(
            sql.SQL("CREATE TEMP TABLE {} (marker text)").format(sql.Identifier(reserved))
        )
        target.execute(
            sql.SQL("INSERT INTO {} VALUES('caller-owned')").format(sql.Identifier(reserved))
        )
        with pytest.raises(ValueError, match="temporary relation already exists"):
            import_objects(target, source_id, changed_keys={"fact": []})
        assert target.execute(
            sql.SQL("SELECT marker FROM {}").format(sql.Identifier(reserved))
        ).fetchone() == {"marker": "caller-owned"}
        target.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(reserved)))
    # Nested caller transactions must also remove only mapper-owned temporary relations.
    with target.transaction():
        result = import_objects(target, source_id, batch_size=1, changed_keys={"fact": []})
        assert result["row_count"] == 0
        assert import_objects(target, source_id, batch_size=1)["row_count"] == 8
    assert (
        target.execute(
            "SELECT id,xmin::text AS version FROM braincore.memories ORDER BY id"
        ).fetchall()
        == versions
    )


def test_finite_tombstone_only_transition_is_imported_with_unchanged_payload_hash(
    typed_source: dict[str, Any],
) -> None:
    target, source_id = typed_source["target"], typed_source["source_id"]
    original = _fact_heads(typed_source)[0]
    vector = "[" + ",".join(["1"] + ["0"] * 383) + "]"
    target.execute(
        """UPDATE braincore.memories SET embedding=%s::public.vector,
           embedding_state='ready',embedding_model='verified-test-model',
           embedding_preprocessing_revision='test-v1' WHERE id=%s""",
        (vector, original["native_memory_id"]),
    )
    old_hash = target.execute(
        "SELECT imported_payload_hash FROM braincore.legacy_object_map "
        "WHERE source_id=%s AND source_table='fact' AND record_key=%s::jsonb",
        (source_id, original["key"]),
    ).fetchone()["imported_payload_hash"]
    _stage_fact_changes(typed_source, [(original["key"], original["payload"], True)])
    result = import_objects(
        target, source_id, batch_size=1, changed_only=True, changed_keys={"fact": [original["key"]]}
    )
    assert result["processed_rows"] == 1 and result["row_count"] == 1
    retired = target.execute(
        "SELECT state,metadata,embedding::text AS vector,embedding_state "
        "FROM braincore.memories WHERE id=%s",
        (original["native_memory_id"],),
    ).fetchone()
    assert retired["state"] == "retired" and retired["metadata"]["legacy"]["source_deleted"]
    assert retired["vector"] == vector and retired["embedding_state"] == "ready"
    assert (
        target.execute(
            "SELECT imported_payload_hash FROM braincore.legacy_object_map "
            "WHERE source_id=%s AND source_table='fact' AND record_key=%s::jsonb",
            (source_id, original["key"]),
        ).fetchone()["imported_payload_hash"]
        == old_hash
    )
    assert (
        import_objects(
            target, source_id, changed_only=True, changed_keys={"fact": [original["key"]]}
        )["processed_rows"]
        == 0
    )


def test_finite_owner_pages_validate_late_invalid_owner_before_native_writes(
    typed_source: dict[str, Any],
) -> None:
    import json

    from braincore_memory.legacy_ledger import stage_records

    target, source_id = typed_source["target"], typed_source["source_id"]
    before = target.execute(
        "SELECT id,xmin::text AS version FROM braincore.memories ORDER BY id"
    ).fetchall()
    changes = []
    for index in range(5):
        external = "00000000-0000-0000-0000-" + format(index + 1, "012x")
        changes.append(
            (
                json.dumps({"fact_id": external}),
                {
                    "fact_id": external,
                    "tenant": "paged-alpha" if index % 2 else "paged-beta",
                    "predicate": "bounded owner discovery",
                    "current_status": "active",
                },
                False,
            )
        )
    late_id = "ffffffff-ffff-ffff-ffff-ffffffffffff"
    late_key = json.dumps({"fact_id": late_id})
    late_payload = {
        "fact_id": late_id,
        "tenant": "",
        "predicate": "late invalid owner",
        "current_status": "active",
    }
    changes.append((late_key, late_payload, False))
    _stage_fact_changes(typed_source, changes)
    owner_pages: list[list[Any]] = []

    class AuditedTarget:
        def __getattr__(self, name: str) -> Any:
            return getattr(target, name)

        def execute(self, query: Any, values: Any = None) -> Any:
            if (
                isinstance(query, str)
                and "WITH selected_keys AS MATERIALIZED" in query
                and values[0] == "fact"
            ):
                owner_pages.append(list(values))
            return target.execute(query, values)

    audited: Any = AuditedTarget()
    keys = {"fact": [key for key, _, _ in changes]}
    with pytest.raises(ValueError, match="explicit original tenants"):
        import_objects(audited, source_id, batch_size=2, changed_keys=keys)
    assert len(owner_pages) == 3 and all(page[-3] == 2 for page in owner_pages)
    assert (
        target.execute(
            "SELECT id,xmin::text AS version FROM braincore.memories ORDER BY id"
        ).fetchall()
        == before
    )
    assert (
        target.execute(
            "SELECT count(*) AS n FROM braincore.tenants WHERE name LIKE 'paged-%'"
        ).fetchone()["n"]
        == 0
    )
    assert (
        target.execute(
            "SELECT to_regclass('pg_temp.braincore_import_keys') AS relation"
        ).fetchone()["relation"]
        is None
    )
    # Correct only the retained late source owner, then retry the complete set.
    with target.transaction():
        stage_records(
            target,
            source_id,
            [
                (
                    "fact",
                    late_key,
                    json.dumps({**late_payload, "tenant": "paged-gamma"}),
                    False,
                    2000,
                    True,
                )
            ],
        )
    owner_pages.clear()
    result = import_objects(audited, source_id, batch_size=2, changed_keys=keys)
    assert result["typed_object_gate"] == "PASS" and result["processed_rows"] == 6
    assert len(owner_pages) == 4 and all(page[-3] == 2 for page in owner_pages)
    assert target.execute(
        "SELECT name FROM braincore.tenants WHERE name LIKE 'paged-%' ORDER BY name"
    ).fetchall() == [{"name": "paged-alpha"}, {"name": "paged-beta"}, {"name": "paged-gamma"}]


def test_finite_owner_query_plan_bounds_keys_before_payload_join(
    typed_source: dict[str, Any],
) -> None:
    from braincore_memory.legacy_objects import _SELECTED_OWNER_PAGE, _stage_keys, prepare_tenants

    target, source_id = typed_source["target"], typed_source["source_id"]
    keys = [row["key"] for row in _fact_heads(typed_source)]
    with target.transaction():
        _stage_keys(target, {"fact": keys})
        query = _SELECTED_OWNER_PAGE.format(cursor_predicate="")
        # EXPLAIN alone does not execute owner discovery or scan source payloads.
        plan = target.execute(
            "EXPLAIN (FORMAT JSON) " + query, ("fact", 2, source_id, "fact")
        ).fetchone()["QUERY PLAN"][0]["Plan"]

        def nodes(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
            yield node
            for child in node.get("Plans", []):
                yield from nodes(child)

        key_cte = next(
            node for node in nodes(plan) if node.get("Subplan Name") == "CTE selected_keys"
        )
        assert key_cte["Node Type"] == "Limit" and key_cte["Plan Rows"] <= 2
        assert "FROM selected_heads h JOIN braincore.legacy_record_versions" in query
        assert "selected_heads AS MATERIALIZED" in query
        assert "ORDER BY k.record_key LIMIT %s" in query
        with pytest.raises(ValueError, match="owner discovery batch size"):
            prepare_tenants(target, source_id, selected_keys=True, owner_batch_size=1001)
        target.execute("DROP TABLE pg_temp.braincore_import_keys")


def test_finite_requested_cursor_crosses_unchanged_page_before_later_change(
    typed_source: dict[str, Any],
) -> None:
    target, source_id = typed_source["target"], typed_source["source_id"]
    original = _fact_heads(typed_source)
    before = target.execute(
        "SELECT id,xmin::text AS version FROM braincore.memories ORDER BY id"
    ).fetchall()
    late = original[-1]
    _stage_fact_changes(
        typed_source,
        [(late["key"], {**late["payload"], "predicate": "late requested change"}, False)],
    )
    report = import_objects(
        target,
        source_id,
        batch_size=2,
        changed_only=True,
        changed_keys={"fact": [row["key"] for row in original]},
    )
    assert report["processed_rows"] == 1 and report["objects"] == {"fact": 3}
    assert report["typed_object_gate"] == "PASS"
    after = target.execute(
        "SELECT id,xmin::text AS version FROM braincore.memories ORDER BY id"
    ).fetchall()
    unchanged = {
        row["id"]: row["version"] for row in before if row["id"] != late["native_memory_id"]
    }
    assert {
        row["id"]: row["version"] for row in after if row["id"] != late["native_memory_id"]
    } == unchanged
    assert (
        target.execute(
            "SELECT content FROM braincore.memories WHERE id=%s", (late["native_memory_id"],)
        )
        .fetchone()["content"]
        .startswith("late requested change")
    )
    assert (
        import_objects(
            target,
            source_id,
            batch_size=2,
            changed_only=True,
            changed_keys={"fact": [row["key"] for row in original]},
        )["processed_rows"]
        == 0
    )


def test_finite_unchanged_keys_still_require_exact_current_identity(
    typed_source: dict[str, Any],
) -> None:
    target, source_id = typed_source["target"], typed_source["source_id"]
    original = _fact_heads(typed_source)
    wrong = original[-1]
    target.execute(
        "UPDATE braincore.memories SET legacy_record_key=jsonb_build_object('fact_id',%s::text) "
        "WHERE id=%s",
        (str(uuid4()), wrong["native_memory_id"]),
    )
    # Hash and source_deleted agree, so the row is a no-op selection. The final
    # bounded page accounting must still reject its wrong canonical identity.
    with pytest.raises(ValueError, match="accounting is incomplete"):
        import_objects(
            target,
            source_id,
            batch_size=2,
            changed_only=True,
            changed_keys={"fact": [row["key"] for row in original]},
        )
    for name in (
        "braincore_import_keys",
        "braincore_import_requested",
        "braincore_import_heads",
        "braincore_import_batch",
    ):
        assert (
            target.execute("SELECT to_regclass(%s) AS relation", ("pg_temp." + name,)).fetchone()[
                "relation"
            ]
            is None
        )


def test_finite_requested_pages_bound_canonical_checks_and_count_missing_heads(
    typed_source: dict[str, Any],
) -> None:
    from braincore_memory.legacy_objects import (
        _RECONCILE_REQUESTED_PAGE,
        _REQUESTED_KEYS_PAGE,
        _UNCHANGED_REQUESTED_HEADS,
        _stage_keys,
    )

    target, source_id = typed_source["target"], typed_source["source_id"]
    keys = [row["key"] for row in _fact_heads(typed_source)]

    def nodes(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield node
        for child in node.get("Plans", []):
            yield from nodes(child)

    with target.transaction():
        _stage_keys(target, {"fact": keys})
        page_plan = target.execute(
            "EXPLAIN (FORMAT JSON) " + _REQUESTED_KEYS_PAGE.format(cursor_predicate=""),
            ("fact", 2),
        ).fetchone()["QUERY PLAN"][0]["Plan"]
        assert page_plan["Node Type"] == "Limit" and page_plan["Plan Rows"] <= 2
        requested = target.execute(
            _REQUESTED_KEYS_PAGE.format(cursor_predicate=""),
            ("fact", 2),
        ).fetchall()
        target.execute(
            "CREATE TEMP TABLE braincore_import_requested "
            "(record_key jsonb PRIMARY KEY) ON COMMIT DROP"
        )
        target.execute(
            "INSERT INTO pg_temp.braincore_import_requested SELECT unnest(%s::jsonb[])",
            ([row["key"] for row in requested],),
        )
        target.execute("ANALYZE pg_temp.braincore_import_requested")
        target.execute(
            """CREATE TEMP TABLE braincore_import_heads ON COMMIT DROP AS
               SELECT h.source_id,h.source_table,h.record_key,h.payload_hash,h.deleted
               FROM pg_temp.braincore_import_requested k JOIN braincore.legacy_record_heads h
                 ON h.record_key=k.record_key WHERE h.source_id=%s AND h.source_table='fact'""",
            (source_id,),
        )
        target.execute("ANALYZE pg_temp.braincore_import_heads")
        for query, params in (
            (_UNCHANGED_REQUESTED_HEADS, None),
            (_RECONCILE_REQUESTED_PAGE, (source_id, "fact")),
        ):
            # EXPLAIN of the DELETE does not remove rows: no ANALYZE requested.
            plan = target.execute("EXPLAIN (FORMAT JSON) " + query, params).fetchone()[
                "QUERY PLAN"
            ][0]["Plan"]
            assert any(n["Node Type"] == "Limit" and n["Plan Rows"] <= 1 for n in nodes(plan))
            canonical = [n for n in nodes(plan) if n.get("Relation Name") == "memories"]
            assert canonical and all(
                n["Node Type"] == "Index Scan" and n["Plan Rows"] <= 1 for n in canonical
            )
        accounting = target.execute(_RECONCILE_REQUESTED_PAGE, (source_id, "fact")).fetchone()
        assert accounting == {"expected": 2, "mapped": 2}
        target.execute(
            "INSERT INTO pg_temp.braincore_import_requested "
            "VALUES(jsonb_build_object('fact_id',%s::text))",
            (str(uuid4()),),
        )
        assert target.execute(_RECONCILE_REQUESTED_PAGE, (source_id, "fact")).fetchone() == {
            "expected": 3,
            "mapped": 2,
        }
        target.execute("DROP TABLE pg_temp.braincore_import_keys")
