"""Compact prior-ID selection preserves authenticated extraction replacement semantics."""

import hashlib
import json
import time
from typing import Any
from uuid import UUID, uuid4

import pytest
from psycopg import sql
from psycopg.errors import InsufficientPrivilege
from psycopg.types.json import Jsonb

from braincore_memory.auth import key_digest
from braincore_memory.legacy_search import configure_authenticated_readers

from .test_legacy_gate import artifact
from .test_legacy_gate import gate_source as _gate_source
from .test_legacy_gate import typed_source as _typed_source

# Every test uses the existing isolated sibling-database fixture.
typed_source = _typed_source
gate_source = _gate_source
HELPER = "SELECT * FROM braincore.compat_extraction_prior(%s,%s) ORDER BY object_type,object_id"
ORIGINAL = """
 SELECT 'fact'::text AS object_type,f.fact_id AS object_id FROM preserve.fact f
 JOIN preserve.extraction_run r ON f.created_run_id=r.run_id
 WHERE r.artifact_id=%s AND f.tenant=%s AND f.current_status='active'
 UNION ALL SELECT 'episode',episode_id FROM preserve.episode
 WHERE primary_artifact_id=%s AND tenant=%s
 UNION ALL SELECT 'segment',segment_id FROM preserve.segment WHERE artifact_id=%s AND tenant=%s
 UNION ALL SELECT 'extraction_run',run_id FROM preserve.extraction_run WHERE artifact_id=%s
 ORDER BY object_type,object_id
"""


def memory(
    g: dict[str, Any],
    kind: str,
    attrs: dict[str, Any],
    *,
    owner: str = "primary",
    scope: str = "visible",
    state: str = "active",
    imported: bool = False,
    ready: bool = True,
    override: dict[str, Any] | None = None,
) -> UUID:
    native, external = uuid4(), uuid4()
    conn = g["target"]
    conn.execute(
        "INSERT INTO braincore.memories(id,tenant_id,object_type,content,dedupe_key,scope,"
        "project_id,state,legacy_attributes_ready,legacy_attributes,legacy_content_hash,"
        "legacy_source_id,legacy_record_key,legacy_external_id) "
        "VALUES(%s,%s,%s,'fixture',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            native,
            g["tenants"][owner]["id"],
            kind,
            hashlib.sha256(native.bytes).digest(),
            Jsonb({"project_id": scope}),
            scope,
            state,
            ready,
            Jsonb(attrs),
            hashlib.sha256(b"fixture").digest(),
            g["source_id"] if imported else None,
            Jsonb({"id": str(external)}) if imported else None,
            external if imported else None,
        ),
    )
    if override is not None:
        conn.execute(
            "INSERT INTO braincore.legacy_client_attributes(memory_id,tenant_id,attributes,"
            "content_hash,current_override) VALUES(%s,%s,%s,%s,true)",
            (
                native,
                g["tenants"][owner]["id"],
                Jsonb(override),
                hashlib.sha256(b"fixture").digest(),
            ),
        )
    return external if imported else native


def prior_run(
    g: dict[str, Any],
    art: UUID,
    *,
    owner: str = "primary",
    scope: str = "visible",
    deleted: bool = False,
    external: UUID | None = None,
    imported: bool = False,
) -> UUID:
    external = external or uuid4()
    attrs = {"artifact_id": str(art), "run_id": str(external), "status": "failed"}
    if imported:
        g["target"].execute(
            "INSERT INTO braincore.legacy_auxiliary_objects(source_id,source_table,record_key,"
            "tenant_id,scope,payload,imported_payload_hash,deleted) "
            "VALUES(%s,'extraction_run',%s,%s,%s,%s,%s,%s)",
            (
                g["source_id"],
                Jsonb({"run_id": str(external)}),
                g["tenants"][owner]["id"],
                Jsonb({"project_id": scope}),
                Jsonb(attrs),
                None,
                deleted,
            ),
        )
    else:
        g["target"].execute(
            "INSERT INTO braincore.legacy_client_controls(tenant_id,object_type,external_id,"
            "scope,fields,deleted) VALUES(%s,'extraction_run',%s,%s,%s,%s)",
            (
                g["tenants"][owner]["id"],
                external,
                Jsonb({"project_id": scope}),
                Jsonb(attrs),
                deleted,
            ),
        )
    return external


def equivalent(g: dict[str, Any], art: UUID) -> list[dict[str, Any]]:
    name = g["tenants"]["primary"]["name"]
    actual = g["target"].execute(HELPER, (name, art)).fetchall()
    expected = g["target"].execute(ORIGINAL, (art, name, art, name, art, name, art)).fetchall()
    assert actual == expected
    return actual


@pytest.mark.parametrize("history", [False, True])
def test_prior_exact_parity_current_overrides_history_scope_and_status(
    gate_source: dict[str, Any],
    history: bool,
) -> None:
    g = gate_source
    art = artifact(g, "prior")[1]
    other = artifact(g, "other")[1]
    run = prior_run(g, art)
    prior_run(g, art, imported=True)
    deleted_run = prior_run(g, art, deleted=True)
    for state in ("active", "suppressed", "retired"):
        memory(g, "fact", {"created_run_id": str(run), "current_status": "active"}, state=state)
        memory(g, "segment", {"artifact_id": str(art)}, state=state)
        memory(g, "episode", {"primary_artifact_id": str(art)}, state=state)
    memory(g, "fact", {"created_run_id": str(deleted_run)})
    memory(g, "fact", {"created_run_id": str(run)}, scope="hidden")
    memory(g, "fact", {"created_run_id": str(run)}, owner="union")
    memory(g, "segment", {"artifact_id": str(art)}, scope="hidden")
    memory(g, "episode", {"primary_artifact_id": str(art)}, owner="unrelated")
    memory(g, "segment", {"artifact_id": str(art)}, imported=True, ready=False)
    memory(
        g, "segment", {"artifact_id": str(other)}, imported=True, override={"artifact_id": str(art)}
    )
    memory(
        g,
        "episode",
        {"primary_artifact_id": str(art)},
        imported=True,
        override={"primary_artifact_id": str(other)},
    )
    memory(
        g,
        "fact",
        {"created_run_id": str(other)},
        imported=True,
        override={"created_run_id": str(run)},
    )
    with g["target"].transaction():
        configure_authenticated_readers(
            g["target"], [g["keys"]["primary"]], include_inactive=history
        )
        rows = equivalent(g, art)
        assert {r["object_type"] for r in rows} == {"fact", "segment", "episode", "extraction_run"}


def test_prior_no_runs_still_returns_independent_segments_and_episodes(
    gate_source: dict[str, Any],
) -> None:
    g = gate_source
    art = artifact(g, "partial")[1]
    expected = [
        memory(g, "segment", {"artifact_id": str(art)}),
        memory(g, "episode", {"primary_artifact_id": str(art)}),
    ]
    memory(g, "fact", {"created_run_id": str(uuid4())})
    with g["target"].transaction():
        configure_authenticated_readers(g["target"], [g["keys"]["primary"]])
        assert {r["object_id"] for r in equivalent(g, art)} == set(expected)


def test_prior_duplicate_run_ids_preserve_original_fact_multiplicity(
    gate_source: dict[str, Any],
) -> None:
    g = gate_source
    art = artifact(g, "duplicates")[1]
    run = prior_run(g, art)
    prior_run(g, art, imported=True, external=run)
    fact = memory(g, "fact", {"created_run_id": str(run)})
    with g["target"].transaction():
        configure_authenticated_readers(g["target"], [g["keys"]["primary"]])
        assert [r["object_id"] for r in equivalent(g, art)].count(fact) == 2


@pytest.mark.parametrize(
    "condition", ["unconfigured", "tenant", "missing", "scope", "union", "alias"]
)
def test_prior_rejects_missing_auth_wrong_owner_scope_and_visible_alias_ambiguity(
    gate_source: dict[str, Any],
    condition: str,
) -> None:
    g = gate_source
    art = artifact(
        g,
        "denied",
        owner="union" if condition == "union" else "primary",
        scope="hidden" if condition == "scope" else "visible",
    )[1]
    if condition == "alias":
        artifact(g, "alias", owner="union", external=art)
    with pytest.raises(InsufficientPrivilege), g["target"].transaction():
        if condition != "unconfigured":
            configure_authenticated_readers(g["target"], [g["keys"]["primary"], g["keys"]["union"]])
        else:
            g["target"].execute("SELECT set_config('braincore.compat_tenant_id','',true)")
        name = g["tenants"]["union" if condition == "tenant" else "primary"]["name"]
        g["target"].execute(HELPER, (name, uuid4() if condition == "missing" else art)).fetchall()


def test_prior_rejects_visible_foreign_run_instead_of_retiring_it(
    gate_source: dict[str, Any],
) -> None:
    g = gate_source
    art = artifact(g, "foreign-run")[1]
    prior_run(g, art, owner="union")
    with (
        pytest.raises(InsufficientPrivilege, match="run is not primary-owned"),
        g["target"].transaction(),
    ):
        configure_authenticated_readers(g["target"], [g["keys"]["primary"], g["keys"]["union"]])
        g["target"].execute(HELPER, (g["tenants"]["primary"]["name"], art)).fetchall()


def test_prior_live_grant_respects_visibility_but_never_changes_primary_owner(
    gate_source: dict[str, Any],
) -> None:
    g = gate_source
    native, art = artifact(g, "granted", scope="hidden")
    g["target"].execute(
        "INSERT INTO braincore.artifact_access_grants(artifact_id,owner_tenant_id,"
        "grantee_tenant_id,"
        "scope,reason) VALUES(%s,%s,%s,%s,'fixture')",
        (
            native,
            g["tenants"]["primary"]["id"],
            g["tenants"]["union"]["id"],
            Jsonb({"project_id": "visible"}),
        ),
    )
    with g["target"].transaction():
        configure_authenticated_readers(g["target"], [g["keys"]["primary"], g["keys"]["union"]])
        assert equivalent(g, art) == []
    g["target"].execute(
        "UPDATE braincore.artifact_access_grants SET revoked_at=now() WHERE artifact_id=%s",
        (native,),
    )
    with pytest.raises(InsufficientPrivilege), g["target"].transaction():
        configure_authenticated_readers(g["target"], [g["keys"]["primary"], g["keys"]["union"]])
        g["target"].execute(HELPER, (g["tenants"]["primary"]["name"], art)).fetchall()


def test_prior_invoker_fixed_path_no_broad_render_or_nested_run_join(
    gate_source: dict[str, Any],
) -> None:
    g = gate_source
    conn = g["target"]
    row = conn.execute(
        "SELECT prosecdef,provolatile,proconfig,prosrc FROM pg_proc WHERE "
        "oid='braincore.compat_extraction_prior(text,uuid)'::regprocedure"
    ).fetchone()
    assert not row["prosecdef"] and row["provolatile"] == "s"
    assert row["proconfig"] == ["search_path=pg_catalog"]
    body = row["prosrc"]
    assert "WITH runs AS MATERIALIZED" in body and "prior AS MATERIALIZED" in body
    assert "FROM candidate_ids selected JOIN braincore.memories" in body
    assert "cardinality(run_ids)>0" in body and "JOIN unnest(run_ids)" in body
    assert all(
        forbidden not in body
        for forbidden in ("m.content", "m.embedding", "digest(", "SELECT *", "LIMIT")
    )
    role = "extraction_denied_" + uuid4().hex
    with conn.transaction(force_rollback=True):
        conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA braincore TO {}").format(sql.Identifier(role)))
        conn.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
        with pytest.raises(InsufficientPrivilege), conn.transaction():
            conn.execute(HELPER, (g["tenants"]["primary"]["name"], uuid4())).fetchall()
    conn.execute(
        "UPDATE braincore.api_keys SET revoked_at=now() WHERE digest=%s",
        (key_digest(g["keys"]["primary"]),),
    )
    with pytest.raises(ValueError, match="credential unavailable"), conn.transaction():
        configure_authenticated_readers(conn, [g["keys"]["primary"]])


def test_prior_native_attribute_fallback_and_external_identity(gate_source: dict[str, Any]) -> None:
    g = gate_source
    art = artifact(g, "fallback")[1]
    native = memory(g, "segment", {}, ready=False, override={"artifact_id": str(art)})
    external = uuid4()
    g["target"].execute(
        "UPDATE braincore.legacy_client_attributes SET current_override=false WHERE memory_id=%s",
        (native,),
    )
    g["target"].execute(
        "INSERT INTO braincore.legacy_client_ids(memory_id,tenant_id,object_type,external_id,"
        "identity) VALUES(%s,%s,'segment',%s,%s)",
        (native, g["tenants"]["primary"]["id"], external, Jsonb({"fixture": str(native)})),
    )
    with g["target"].transaction():
        configure_authenticated_readers(g["target"], [g["keys"]["primary"]])
        assert [r["object_id"] for r in equivalent(g, art)] == [external]


def test_prior_refuses_unowned_operator_run_even_for_admin(gate_source: dict[str, Any]) -> None:
    g = gate_source
    art = artifact(g, "operator-run")[1]
    run = uuid4()
    g["target"].execute(
        "INSERT INTO braincore.legacy_auxiliary_objects(source_id,source_table,record_key,"
        "tenant_id,scope,payload) VALUES(%s,'extraction_run',%s,NULL,NULL,%s)",
        (
            g["source_id"],
            Jsonb({"run_id": str(run)}),
            Jsonb({"run_id": str(run), "artifact_id": str(art)}),
        ),
    )
    key = "bck_extraction_admin_" + uuid4().hex
    g["target"].execute(
        "INSERT INTO braincore.api_keys(tenant_id,name,prefix,digest,roles) "
        "VALUES(%s,%s,%s,%s,ARRAY['admin'])",
        (g["tenants"]["primary"]["id"], key, key, key_digest(key)),
    )
    with (
        pytest.raises(InsufficientPrivilege, match="run is not primary-owned"),
        g["target"].transaction(),
    ):
        configure_authenticated_readers(g["target"], [key])
        # This run was visible to the old unqualified run read, but cannot be
        # safely retired through a primary-owner extraction packet.
        assert (
            g["target"]
            .execute("SELECT run_id FROM preserve.extraction_run WHERE artifact_id=%s", (art,))
            .fetchone()["run_id"]
            == run
        )
        g["target"].execute(HELPER, (g["tenants"]["primary"]["name"], art)).fetchall()


@pytest.mark.parametrize("has_runs", [False, True])
def test_prior_compact_query_plan_materializes_once_on_owned_rows(
    gate_source: dict[str, Any],
    has_runs: bool,
) -> None:
    g = gate_source
    art = artifact(g, "plan")[1]
    run = prior_run(g, art) if has_runs else uuid4()
    for kind, field, value in (
        ("fact", "created_run_id", run),
        ("segment", "artifact_id", art),
        ("episode", "primary_artifact_id", art),
    ):
        for _ in range(4):
            memory(g, kind, {field: str(value)})
    conn = g["target"]
    body = conn.execute(
        "SELECT prosrc FROM pg_proc WHERE "
        "oid='braincore.compat_extraction_prior(text,uuid)'::regprocedure"
    ).fetchone()["prosrc"]
    statement = body[body.index("WITH candidate_ids AS MATERIALIZED") : body.rindex("\nEND;")]
    statement = statement.strip().removesuffix(";")
    assert ";" not in statement
    # Explain the actual return-query body with safe literal fixture bindings;
    # EXPLAIN cannot expose the nested body of a PL/pgSQL Function Scan itself.
    statement = sql.SQL(statement).as_string(conn)
    for symbol, value in (
        ("primary_id", g["tenants"]["primary"]["id"]),
        ("run_ids", [run] if has_runs else []),
        ("requested_artifact", art),
    ):
        literal = sql.Literal(value).as_string(conn)
        if symbol == "run_ids":
            literal += "::uuid[]"
        statement = statement.replace(symbol, literal)
    with conn.transaction():
        configure_authenticated_readers(conn, [g["keys"]["primary"]])
        document = conn.execute("EXPLAIN (FORMAT JSON, VERBOSE) " + statement).fetchone()
        plan = next(iter(document.values()))[0]["Plan"]
        nodes = []

        def visit(node: dict[str, Any]) -> None:
            nodes.append(node)
            for child in node.get("Plans", []):
                visit(child)

        visit(plan)
        assert sum(n.get("Subplan Name") == "CTE prior" for n in nodes) == 1
        assert not any(n.get("Relation Name") == "legacy_auxiliary_objects" for n in nodes)
        assert not any("embedding" in str(n.get("Output", [])) for n in nodes)
        started = time.perf_counter()
        result = conn.execute(HELPER, (g["tenants"]["primary"]["name"], art)).fetchall()
        if not has_runs:
            assert not any(row["object_type"] == "fact" for row in result)
        elapsed = (time.perf_counter() - started) * 1000
        print(
            "PRIOR_FIXTURE "
            + json.dumps(
                {
                    "has_runs": has_runs,
                    "result_count": len(result),
                    "helper_ms": round(elapsed, 3),
                    "relation_scan_estimates": [
                        {
                            "relation": n["Relation Name"],
                            "node": n["Node Type"],
                            "rows": n["Plan Rows"],
                        }
                        for n in nodes
                        if "Relation Name" in n
                    ],
                }
            )
        )


def test_prior_parent_lookup_bounds_work_with_large_unrelated_population(
    gate_source: dict[str, Any],
) -> None:
    """Real natural plans select parent IDs before loading current attributes."""
    g = gate_source
    conn = g["target"]
    art = artifact(g, "scale")[1]
    unrelated = artifact(g, "unrelated-scale")[1]
    run = prior_run(g, art)
    wanted = {
        memory(g, "fact", {"created_run_id": str(run)}),
        memory(g, "segment", {"artifact_id": str(art)}),
        memory(g, "episode", {"primary_artifact_id": str(art)}),
        memory(g, "segment", {}, ready=False, override={"artifact_id": str(art)}),
    }
    for kind, field in (
        ("fact", "created_run_id"),
        ("segment", "artifact_id"),
        ("episode", "primary_artifact_id"),
    ):
        conn.execute(
            "WITH generated AS (SELECT gen_random_uuid() AS id FROM generate_series(1,6000)) "
            "INSERT INTO braincore.memories(id,tenant_id,object_type,content,dedupe_key,scope,"
            "project_id,state,legacy_attributes_ready,legacy_attributes,legacy_content_hash) "
            "SELECT id,%s,%s,'unrelated fixture',public.digest(id::text,'sha256'),%s,'visible',"
            "'active',true,jsonb_build_object(%s::text,%s::text),public.digest('fixture','sha256') "
            "FROM generated",
            (
                g["tenants"]["primary"]["id"],
                kind,
                Jsonb({"project_id": "visible"}),
                field,
                str(unrelated),
            ),
        )
    conn.execute(
        "WITH generated AS (SELECT gen_random_uuid() AS id FROM generate_series(1,6000)),"
        "inserted AS (INSERT INTO braincore.memories(id,tenant_id,object_type,content,"
        "dedupe_key,scope,project_id,state,legacy_attributes_ready) "
        "SELECT id,%s,'segment','unrelated fallback',public.digest(id::text,'sha256'),%s,"
        "'visible','active',false FROM generated RETURNING id,tenant_id) "
        "INSERT INTO braincore.legacy_client_attributes(memory_id,tenant_id,attributes,"
        "content_hash,current_override) SELECT id,tenant_id,"
        "jsonb_build_object('artifact_id',%s::text),"
        "public.digest('fixture','sha256'),true FROM inserted",
        (g["tenants"]["primary"]["id"], Jsonb({"project_id": "visible"}), str(unrelated)),
    )
    for relation in ("memories", "legacy_client_attributes", "legacy_client_ids"):
        conn.execute(sql.SQL("ANALYZE {}").format(sql.Identifier("braincore", relation)))
    body = conn.execute(
        "SELECT prosrc FROM pg_proc WHERE "
        "oid='braincore.compat_extraction_prior(text,uuid)'::regprocedure"
    ).fetchone()["prosrc"]
    statement = body[body.index("WITH candidate_ids AS MATERIALIZED") : body.rindex("\nEND;")]
    statement = statement.strip().removesuffix(";")
    for symbol, value in (
        ("primary_id", g["tenants"]["primary"]["id"]),
        ("run_ids", [run]),
        ("requested_artifact", art),
    ):
        literal = sql.Literal(value).as_string(conn)
        if symbol == "run_ids":
            literal += "::uuid[]"
        statement = statement.replace(symbol, literal)
    with conn.transaction():
        configure_authenticated_readers(conn, [g["keys"]["primary"]])
        conn.execute("SET LOCAL statement_timeout='1s'")
        plan = next(
            iter(
                conn.execute("EXPLAIN (ANALYZE, FORMAT JSON, VERBOSE) " + statement)
                .fetchone()
                .values()
            )
        )[0]["Plan"]
        nodes = []

        def visit(node: dict[str, Any]) -> None:
            nodes.append(node)
            for child in node.get("Plans", []):
                visit(child)

        visit(plan)
        prior = [n for n in nodes if n.get("Subplan Name") == "CTE prior"]
        print(
            "PRIOR_SCALE_NODES "
            + json.dumps(
                [
                    {
                        "node": n["Node Type"],
                        "relation": n.get("Relation Name"),
                        "index": n.get("Index Name"),
                        "estimated_rows": n["Plan Rows"],
                        "actual_rows": n.get("Actual Rows"),
                        "subplan": n.get("Subplan Name"),
                    }
                    for n in nodes
                ]
            )
        )
        assert len(prior) == 1 and prior[0]["Actual Rows"] == 4
        assert not any(
            n.get("Node Type") == "Seq Scan"
            and n.get("Relation Name") in {"memories", "legacy_client_attributes"}
            for n in nodes
        )
        assert any(n.get("Index Name") == "memories_extraction_parent_idx" for n in nodes)
        assert any(n.get("Index Name") == "legacy_client_prior_artifact_idx" for n in nodes)
        rows = equivalent(g, art)
        assert {r["object_id"] for r in rows} == wanted | {run}
        print(
            "PRIOR_SCALE "
            + json.dumps(
                {
                    "unrelated_memories": 24000,
                    "unrelated_overrides": 6000,
                    "prior_estimated_rows": prior[0]["Plan Rows"],
                    "result_count": len(rows),
                    "natural_indexes": sorted(
                        {n["Index Name"] for n in nodes if "Index Name" in n}
                    ),
                }
            )
        )
