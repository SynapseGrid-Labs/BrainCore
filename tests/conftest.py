import os
from collections.abc import AsyncIterator, Iterator
from typing import Any
from uuid import uuid4

import httpx
import psycopg
import pytest
import pytest_asyncio
from psycopg import Binary, sql
from psycopg.rows import dict_row

from braincore_memory.api import app
from braincore_memory.auth import key_digest
from braincore_memory.config import get_settings
from braincore_memory.legacy_capture import CAPTURE_SCHEMA
from braincore_memory.migrate import apply_migrations, bootstrap_admin

ADMIN_KEY = "bck_test_0123456789abcdefghijklmnopqrstuvwxyz"
SECOND_KEY = "bck_second_0123456789abcdefghijklmnopqrstuvwxyz"


@pytest.fixture(scope="session", autouse=True)
def isolated_database() -> None:
    if os.getenv("BRAINCORE_E2E") != "1":
        return
    get_settings.cache_clear()
    apply_migrations()
    settings = get_settings()
    with psycopg.connect(settings.database_url.get_secret_value()) as conn, conn.cursor() as cursor:
        cursor.execute("TRUNCATE braincore.tenants CASCADE")
        conn.commit()
    bootstrap_admin()


@pytest_asyncio.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    if os.getenv("BRAINCORE_E2E") != "1":
        pytest.skip("set BRAINCORE_E2E=1 to run database integration tests")
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as test_client:
            yield test_client


def auth_headers(key: str = ADMIN_KEY, idempotency: str | None = None) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {key}"}
    if idempotency:
        headers["Idempotency-Key"] = idempotency
    return headers


def create_second_tenant() -> None:
    settings = get_settings()
    with psycopg.connect(settings.database_url.get_secret_value()) as conn, conn.cursor() as cursor:
        cursor.execute("INSERT INTO braincore.tenants(name) VALUES ('second') RETURNING id")
        tenant_id = cursor.fetchone()[0]
        cursor.execute(
            """INSERT INTO braincore.api_keys(tenant_id,name,prefix,digest,roles,scope)
               VALUES (%s,'second-test',%s,%s,ARRAY['read','write'],%s)""",
            (
                tenant_id,
                SECOND_KEY[:12],
                Binary(key_digest(SECOND_KEY)),
                '{"user_id":"alice"}',
            ),
        )
        conn.commit()


@pytest.fixture
def capture_source() -> Iterator[tuple[psycopg.Connection[Any], str]]:
    if os.getenv("BRAINCORE_E2E") != "1":
        pytest.skip("requires the disposable integration database")
    with psycopg.connect(
        get_settings().database_url.get_secret_value(), autocommit=True, row_factory=dict_row
    ) as c:
        assert c.execute("SELECT current_database() AS name").fetchone()["name"] == "braincore_test"
        assert (
            c.execute("SELECT to_regnamespace(%s) AS ns", (CAPTURE_SCHEMA,)).fetchone()["ns"]
            is None
        )
        schema = "capture_test_" + uuid4().hex
        c.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        c.execute(
            sql.SQL("CREATE TABLE {} (part int,id int,payload text,PRIMARY KEY(part,id))").format(
                sql.Identifier(schema, "records")
            )
        )
        try:
            yield c, schema
        finally:
            c.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(CAPTURE_SCHEMA))
            )
            c.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
