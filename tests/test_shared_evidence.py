import json
import secrets
from typing import Any
from uuid import uuid4

import httpx
import psycopg
import pytest
from psycopg import Binary
from psycopg.rows import dict_row

from braincore_memory.auth import key_digest
from braincore_memory.config import get_settings
from tests.conftest import ADMIN_KEY, auth_headers


def shared_case() -> dict[str, Any]:
    suffix = uuid4().hex
    recipient_key = "bck_reader_" + secrets.token_hex(24)
    recipient_admin = "bck_admin_" + secrets.token_hex(24)
    scope = {"project_id": "shared-" + suffix}
    with psycopg.connect(get_settings().database_url.get_secret_value(), row_factory=dict_row) as c:
        owner = c.execute("SELECT id FROM braincore.tenants WHERE name='default'").fetchone()["id"]
        recipient = c.execute(
            "INSERT INTO braincore.tenants(name) VALUES (%s) RETURNING id", ("share-" + suffix,)
        ).fetchone()["id"]
        for key, roles, binding in [
            (recipient_key, ["read"], json.dumps(scope)),
            (recipient_admin, ["read", "write", "admin"], None),
        ]:
            c.execute(
                """INSERT INTO braincore.api_keys(tenant_id,name,prefix,digest,roles,scope)
                   VALUES (%s,'sharing-test',%s,%s,%s,%s)""",
                (recipient, key[:24], Binary(key_digest(key)), roles, binding),
            )
        artifact = c.execute(
            """INSERT INTO braincore.artifacts
               (tenant_id,source_type,content,content_sha256,scope)
               VALUES (%s,'test','private shared quote',%s,%s) RETURNING id""",
            (owner, Binary(b"a" * 32), json.dumps(scope)),
        ).fetchone()["id"]
        memory = c.execute(
            """INSERT INTO braincore.memories
               (tenant_id,content,dedupe_key,project_id,scope)
               VALUES (%s,'recipient-owned statement',%s,%s,%s) RETURNING id""",
            (recipient, Binary(suffix.encode()), scope["project_id"], json.dumps(scope)),
        ).fetchone()["id"]
    return {
        "owner": owner,
        "recipient": recipient,
        "artifact": artifact,
        "memory": memory,
        "key": recipient_key,
        "admin_key": recipient_admin,
        "scope": scope,
    }


def link(case: dict[str, Any], grant: str | None) -> None:
    with psycopg.connect(get_settings().database_url.get_secret_value()) as c:
        c.execute(
            """INSERT INTO braincore.memory_evidence
               (tenant_id,memory_id,artifact_id,artifact_tenant_id,access_grant_id,quote)
               VALUES (%s,%s,%s,%s,%s,'private shared quote')""",
            (case["recipient"], case["memory"], case["artifact"], case["owner"], grant),
        )


async def test_shared_evidence_owner_authority_retry_and_revocation(
    client: httpx.AsyncClient,
) -> None:
    case = shared_case()
    with pytest.raises(psycopg.errors.CheckViolation):
        link(case, None)
    path = f"/v1/artifacts/{case['artifact']}/grants"
    body = {
        "grantee_tenant_id": str(case["recipient"]),
        "scope": case["scope"],
        "reason": "Verified source sharing",
    }
    assert (
        await client.post(
            path,
            json={**body, "grantee_tenant_id": str(case["owner"])},
            headers=auth_headers(case["admin_key"], "not-owner"),
        )
    ).status_code == 404
    assert (
        await client.post(path, json=body, headers=auth_headers(case["key"], "not-admin"))
    ).status_code == 403
    headers = auth_headers(ADMIN_KEY, "grant-" + str(case["artifact"]))
    response = await client.post(path, json=body, headers=headers)
    assert response.status_code == 201, response.text
    grant = response.json()["grant_id"]
    retry = await client.post(path, json=body, headers=headers)
    assert retry.json()["grant_id"] == grant and retry.json()["reused"]
    assert (
        await client.post(path, json={**body, "reason": "changed"}, headers=headers)
    ).status_code == 409
    link(case, grant)
    explain = f"/v1/memories/{case['memory']}/explain"
    result = await client.get(explain, headers=auth_headers(case["key"]))
    assert result.status_code == 200
    assert [e["quote"] for e in result.json()["evidence"]] == ["private shared quote"]
    revoke = f"/v1/evidence-grants/{grant}/revoke"
    revoke_headers = auth_headers(ADMIN_KEY, "revoke-" + grant)
    assert (
        await client.post(revoke, json={"reason": "Sharing ended"}, headers=revoke_headers)
    ).status_code == 200
    retry = await client.post(revoke, json={"reason": "Sharing ended"}, headers=revoke_headers)
    assert retry.json()["reused"]
    result = await client.get(explain, headers=auth_headers(case["key"]))
    assert result.json()["evidence"] == []
    assert "private shared quote" not in result.text
    with psycopg.connect(get_settings().database_url.get_secret_value()) as c:
        assert (
            c.execute(
                "SELECT tenant_id FROM braincore.artifacts WHERE id=%s", (case["artifact"],)
            ).fetchone()[0]
            == case["owner"]
        )
        events = c.execute(
            """SELECT action FROM braincore.audit_events WHERE target_id=%s
                              AND action LIKE 'evidence.%%' ORDER BY created_at""",
            (case["artifact"],),
        ).fetchall()
        assert [e[0] for e in events] == ["evidence.grant", "evidence.revoke"]
        with pytest.raises(psycopg.errors.CheckViolation):
            c.execute(
                "UPDATE braincore.memory_evidence SET quote='changed' WHERE memory_id=%s",
                (case["memory"],),
            )


async def test_sharing_scope_expiry_and_immutable_identity(client: httpx.AsyncClient) -> None:
    case = shared_case()
    path = f"/v1/artifacts/{case['artifact']}/grants"
    body = {
        "grantee_tenant_id": str(case["recipient"]),
        "scope": {"project_id": "other"},
        "reason": "Scope limited grant",
    }
    response = await client.post(
        path, json=body, headers=auth_headers(ADMIN_KEY, "scope-" + str(case["artifact"]))
    )
    assert response.status_code == 201, response.text
    with pytest.raises(psycopg.errors.CheckViolation):
        link(case, response.json()["grant_id"])
    with (
        psycopg.connect(get_settings().database_url.get_secret_value()) as c,
        pytest.raises(psycopg.errors.CheckViolation),
    ):
        c.execute(
            "UPDATE braincore.artifact_access_grants SET scope=%s WHERE id=%s",
            (json.dumps(case["scope"]), response.json()["grant_id"]),
        )
    with psycopg.connect(get_settings().database_url.get_secret_value()) as c:
        expired = c.execute(
            """INSERT INTO braincore.artifact_access_grants
               (artifact_id,owner_tenant_id,grantee_tenant_id,scope,reason,created_at,expires_at)
               VALUES (%s,%s,%s,%s,'Historical grant',now()-interval '2 days',
                       now()-interval '1 day') RETURNING id""",
            (case["artifact"], case["owner"], case["recipient"], json.dumps(case["scope"])),
        ).fetchone()[0]
    with pytest.raises(psycopg.errors.CheckViolation):
        link(case, str(expired))
    valid = await client.post(
        path,
        json={**body, "scope": case["scope"]},
        headers=auth_headers(ADMIN_KEY, "valid-" + str(case["artifact"])),
    )
    link(case, valid.json()["grant_id"])
    # Changing the memory's scope cannot retain evidence authorized for its former scope.
    with psycopg.connect(get_settings().database_url.get_secret_value()) as c:
        c.execute(
            "UPDATE braincore.memories SET project_id='other',scope=%s WHERE id=%s",
            (json.dumps({"project_id": "other"}), case["memory"]),
        )
    result = await client.get(
        f"/v1/memories/{case['memory']}/explain", headers=auth_headers(case["admin_key"])
    )
    assert result.json()["evidence"] == []


async def test_grant_validation_rejects_unbounded_scope_and_unowned_reads(
    client: httpx.AsyncClient,
) -> None:
    case = shared_case()
    path = f"/v1/artifacts/{case['artifact']}/grants"
    body = {"grantee_tenant_id": str(case["recipient"]), "scope": {}, "reason": "test"}
    assert (
        await client.post(
            path, json=body, headers=auth_headers(ADMIN_KEY, "bad-" + str(case["artifact"]))
        )
    ).status_code == 422
    assert (
        await client.get(f"/v1/memories/{case['memory']}/explain", headers=auth_headers(ADMIN_KEY))
    ).status_code == 404
    with (
        psycopg.connect(get_settings().database_url.get_secret_value()) as c,
        pytest.raises(psycopg.errors.CheckViolation),
    ):
        c.execute(
            """INSERT INTO braincore.artifact_access_grants
                     (artifact_id,owner_tenant_id,grantee_tenant_id,scope,reason)
                     VALUES (%s,%s,%s,'{"project_id":null}','Invalid scope')""",
            (case["artifact"], case["owner"], case["recipient"]),
        )
