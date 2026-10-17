"""Stranded gate equivalence on isolated canonical compatibility fixtures."""

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from psycopg.errors import InsufficientPrivilege
from psycopg.types.json import Jsonb

from braincore_memory.auth import key_digest
from braincore_memory.legacy_search import configure_authenticated_readers

from .test_legacy_objects import typed_source as _typed_source

typed_source = _typed_source

ORIGINAL = """
 SELECT a.artifact_id,a.source_key,a.preservation_state,a.original_path,a.discovered_at
 FROM preserve.artifact a
 WHERE a.tenant=%s AND a.source_type IN ('codex_session','codex_shared')
 AND a.preservation_state NOT IN ('blocked'::preserve.preservation_state,
   'failed'::preserve.preservation_state,'pending_escalation'::preserve.preservation_state)
 AND NOT EXISTS (SELECT 1 FROM preserve.extraction_run own_run
   WHERE own_run.artifact_id=a.artifact_id AND own_run.status='success'::preserve.extraction_status)
 AND NOT EXISTS (SELECT 1 FROM preserve.artifact newer
   JOIN preserve.extraction_run newer_run ON newer_run.artifact_id=newer.artifact_id
     AND newer_run.status='success'::preserve.extraction_status
   WHERE newer.tenant=a.tenant AND newer.source_type=a.source_type
     AND regexp_replace(newer.source_key,':[0-9]{4}-[0-9]{2}-[0-9]{2}$','')
       =regexp_replace(a.source_key,':[0-9]{4}-[0-9]{2}-[0-9]{2}$','')
     AND newer.discovered_at>a.discovered_at)
 ORDER BY a.discovered_at ASC,a.artifact_id ASC
"""
HELPER = "SELECT * FROM braincore.compat_gate_stranded(%s) ORDER BY discovered_at,artifact_id"
DAY = datetime(2020, 1, 1, tzinfo=UTC)


@pytest.fixture
def gate_source(typed_source: dict[str, Any]) -> dict[str, Any]:
    conn = typed_source["target"]
    tenants = {}
    keys = {}
    for kind in ("primary", "union", "unrelated"):
        name = "gate-" + kind + "-" + uuid4().hex
        row = conn.execute(
            "INSERT INTO braincore.tenants(name) VALUES(%s) RETURNING id", (name,)
        ).fetchone()
        tenants[kind] = {"id": row["id"], "name": name}
        key = "bck_gate_test_" + uuid4().hex
        conn.execute(
            "INSERT INTO braincore.api_keys(tenant_id,name,prefix,digest,roles,scope) "
            "VALUES(%s,%s,%s,%s,ARRAY['read'],%s)",
            (row["id"], name, key, key_digest(key), Jsonb({"project_id": "visible"})),
        )
        keys[kind] = key
    return {**typed_source, "tenants": tenants, "keys": keys}


def artifact(
    g: dict[str, Any],
    key: str | None,
    *,
    owner: str = "primary",
    scope: str = "visible",
    source_type: str = "codex_session",
    state: str | None = "discovered",
    at: datetime | None = DAY,
    deleted: bool = False,
    external: UUID | None = None,
) -> tuple[UUID, UUID]:
    native = uuid4()
    external = external or uuid4()
    g["target"].execute(
        "INSERT INTO braincore.artifacts(id,tenant_id,source_type,content,content_sha256,scope,"
        "metadata,legacy_attributes,legacy_attributes_ready) "
        "VALUES(%s,%s,%s,'fixture',%s,%s,%s,%s,true)",
        (
            native,
            g["tenants"][owner]["id"],
            source_type,
            hashlib.sha256(native.bytes).digest(),
            Jsonb({"project_id": scope}),
            Jsonb({"legacy": {"source_deleted": deleted}}),
            Jsonb(
                {
                    "artifact_id": str(external),
                    "source_key": key,
                    "original_path": key,
                    "preservation_state": state,
                    "discovered_at": at.isoformat() if at else None,
                }
            ),
        ),
    )
    return native, external


def run(
    g: dict[str, Any],
    external: UUID,
    *,
    status: str = "success",
    scope: str = "visible",
    owner: str = "primary",
    deleted: bool = False,
) -> None:
    g["target"].execute(
        "INSERT INTO braincore.legacy_client_controls(tenant_id,object_type,external_id,"
        "scope,fields,"
        "deleted) VALUES(%s,'extraction_run',%s,%s,%s,%s)",
        (
            g["tenants"][owner]["id"],
            uuid4(),
            Jsonb({"project_id": scope}),
            Jsonb({"artifact_id": str(external), "status": status}),
            deleted,
        ),
    )


def equivalent(g: dict[str, Any]) -> list[dict[str, Any]]:
    conn = g["target"]
    name = g["tenants"]["primary"]["name"]
    original = conn.execute(ORIGINAL, (name,)).fetchall()
    actual = conn.execute(HELPER, (name,)).fetchall()
    assert actual == original
    return actual


def test_stranded_gate_preserves_nulls_strict_dates_duplicates_and_full_order(
    gate_source: dict[str, Any],
) -> None:
    g = gate_source
    expected = []
    expected.append(artifact(g, "orphan")[1])
    artifact(g, "not-codex", source_type="document")
    for state in ("blocked", "failed", "pending_escalation", None):
        artifact(g, "excluded-" + str(state), state=state)
    own = artifact(g, "own-success")[1]
    run(g, own)
    run(g, own)  # Duplicate successful runs must not multiply output rows.
    failed = artifact(g, "own-failed")[1]
    expected.append(failed)
    run(g, failed, status="failed")
    artifact(g, "normalized:2020-01-01")
    newer = artifact(g, "normalized:2021-01-01", at=DAY + timedelta(days=1), state="blocked")[1]
    run(g, newer)  # Outer state exclusions must not hide successful suppressors.
    equal = artifact(g, "equal:2020-01-01")[1]
    expected.append(equal)
    run(g, artifact(g, "equal:2021-01-01", at=DAY)[1])
    expected.append(artifact(g, "different-type:2020-01-01")[1])
    run(
        g,
        artifact(
            g, "different-type:2021-01-01", source_type="codex_shared", at=DAY + timedelta(days=1)
        )[1],
    )
    expected.append(artifact(g, None)[1])
    run(g, artifact(g, None, at=DAY + timedelta(days=1))[1])
    expected.append(artifact(g, "null-old-time", at=None)[1])
    run(g, artifact(g, "null-old-time", at=DAY + timedelta(days=1))[1])
    expected.append(artifact(g, "null-new-time")[1])
    run(g, artifact(g, "null-new-time", at=None)[1])
    expected.append(artifact(g, "not-date:old")[1])
    run(g, artifact(g, "not-date:new", at=DAY + timedelta(days=1))[1])
    duplicate = uuid4()
    artifact(g, "duplicate-a", external=duplicate)
    artifact(g, "duplicate-b", external=duplicate)
    expected.extend([duplicate, duplicate])
    with g["target"].transaction():
        configure_authenticated_readers(g["target"], [g["keys"]["primary"]])
        actual = equivalent(g)
        assert sorted(row["artifact_id"].int for row in actual) == sorted(x.int for x in expected)
        assert actual[-1]["discovered_at"] is None


@pytest.mark.parametrize("configured", [False, True])
def test_stranded_gate_refuses_unconfigured_or_nonprimary_tenant(
    gate_source: dict[str, Any],
    configured: bool,
) -> None:
    g = gate_source
    with pytest.raises(InsufficientPrivilege), g["target"].transaction():
        if configured:
            configure_authenticated_readers(g["target"], [g["keys"]["primary"], g["keys"]["union"]])
        else:
            g["target"].execute("SELECT set_config('braincore.compat_tenant_id','',true)")
        g["target"].execute(HELPER, (g["tenants"]["union"]["name"],)).fetchall()


def test_stranded_gate_scope_and_success_visibility_remain_identical(
    gate_source: dict[str, Any],
) -> None:
    g = gate_source
    visible = artifact(g, "visible")[1]
    artifact(g, "hidden", scope="hidden")
    run(g, visible, scope="hidden")  # An invisible success must not suppress visible work.
    with g["target"].transaction():
        configure_authenticated_readers(g["target"], [g["keys"]["primary"]])
        assert [row["artifact_id"] for row in equivalent(g)] == [visible]


def test_stranded_gate_preserves_grants_read_union_and_tenant_boundary(
    gate_source: dict[str, Any],
) -> None:
    g = gate_source
    conn = g["target"]
    live_native, live = artifact(g, "live-grant", scope="hidden")
    revoked_native, _ = artifact(g, "revoked-grant", scope="hidden")
    expired_native, _ = artifact(g, "expired-grant", scope="hidden")
    foreign_native, _ = artifact(g, "foreign-owner", owner="unrelated", scope="hidden")
    for native, owner, grantee, expired, revoked in (
        (live_native, "primary", "union", False, False),
        (revoked_native, "primary", "union", False, True),
        (expired_native, "primary", "union", True, False),
        (foreign_native, "unrelated", "primary", False, False),
    ):
        conn.execute(
            "INSERT INTO braincore.artifact_access_grants(artifact_id,owner_tenant_id,"
            "grantee_tenant_id,scope,reason,created_at,expires_at,revoked_at) "
            "VALUES(%s,%s,%s,%s,'fixture',now()-interval '2 days',"
            "CASE WHEN %s THEN now()-interval '1 day' END,CASE WHEN %s THEN now() END)",
            (
                native,
                g["tenants"][owner]["id"],
                g["tenants"][grantee]["id"],
                Jsonb({"project_id": "visible"}),
                expired,
                revoked,
            ),
        )
    with conn.transaction():
        configure_authenticated_readers(conn, [g["keys"]["primary"]])
        assert equivalent(g) == []
    with conn.transaction():
        configure_authenticated_readers(conn, [g["keys"]["primary"], g["keys"]["union"]])
        assert [row["artifact_id"] for row in equivalent(g)] == [live]
    run(g, live, owner="union")
    with conn.transaction():
        configure_authenticated_readers(conn, [g["keys"]["primary"], g["keys"]["union"]])
        assert equivalent(g) == []


@pytest.mark.parametrize("history", [False, True])
def test_stranded_gate_preserves_deleted_artifact_and_run_history(
    gate_source: dict[str, Any],
    history: bool,
) -> None:
    g = gate_source
    deleted = artifact(g, "deleted-artifact", deleted=True)[1]
    active = artifact(g, "deleted-run")[1]
    run(g, active, deleted=True)
    with g["target"].transaction():
        configure_authenticated_readers(
            g["target"], [g["keys"]["primary"]], include_inactive=history
        )
        assert [row["artifact_id"] for row in equivalent(g)] == ([deleted] if history else [active])


def test_stranded_gate_is_invoker_fixed_path_and_rejects_revoked_auth(
    gate_source: dict[str, Any],
) -> None:
    g = gate_source
    conn = g["target"]
    row = conn.execute(
        "SELECT prosecdef,provolatile,proconfig FROM pg_proc "
        "WHERE oid='braincore.compat_gate_stranded(text)'::regprocedure"
    ).fetchone()
    assert not row["prosecdef"] and row["provolatile"] == "s"
    assert row["proconfig"] == ["search_path=pg_catalog"]
    conn.execute(
        "UPDATE braincore.api_keys SET revoked_at=now() WHERE digest=%s",
        (key_digest(g["keys"]["primary"]),),
    )
    with pytest.raises(ValueError, match="credential unavailable"), conn.transaction():
        configure_authenticated_readers(conn, [g["keys"]["primary"]])
