import os
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from braincore_memory.config import get_settings
from braincore_memory.migrate import apply_migrations, migration_root


def test_fresh_migrations_with_product_schema_on_search_path(monkeypatch):
    if os.getenv("BRAINCORE_E2E") != "1":
        pytest.skip("requires disposable integration database")
    original = get_settings().database_url.get_secret_value()
    name = "migration_test_" + uuid4().hex
    with psycopg.connect(original, autocommit=True) as admin:
        assert admin.execute("SELECT current_database()").fetchone()[0] == "braincore_test"
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            destination = make_conninfo(
                original, dbname=name, options="-csearch_path=braincore,public"
            )
            monkeypatch.setenv("BRAINCORE_DATABASE_URL", destination)
            get_settings.cache_clear()
            apply_migrations()
            with psycopg.connect(destination) as check:
                first = check.execute(
                    "SELECT version,checksum FROM braincore.schema_migrations ORDER BY version"
                ).fetchall()
                assert len(first) == len(list(migration_root().glob("*.sql")))
                assert check.execute("SHOW search_path").fetchone()[0] == "braincore,public"
            apply_migrations()
            with psycopg.connect(destination) as check:
                assert (
                    check.execute(
                        "SELECT version,checksum FROM braincore.schema_migrations ORDER BY version"
                    ).fetchall()
                    == first
                )
        finally:
            get_settings.cache_clear()
            monkeypatch.setenv("BRAINCORE_DATABASE_URL", original)
            admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))
