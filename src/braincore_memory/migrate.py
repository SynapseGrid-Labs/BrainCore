import hashlib
from pathlib import Path

import psycopg
from psycopg import Binary

from .auth import key_digest
from .config import get_settings


def migration_root() -> Path:
    candidate = Path(__file__).resolve().parent / "migrations"
    if candidate.is_dir():
        return candidate
    candidate = Path(__file__).resolve().parents[2] / "migrations"
    if candidate.is_dir():
        return candidate
    candidate = Path.cwd() / "migrations"
    if candidate.is_dir():
        return candidate
    raise RuntimeError("migrations directory was not found")


def apply_migrations() -> None:
    settings = get_settings()
    with psycopg.connect(settings.database_url.get_secret_value(), autocommit=False) as conn:
        with conn.cursor() as cursor:
            for path in sorted(migration_root().glob("*.sql")):
                content = path.read_text(encoding="utf-8")
                checksum = hashlib.sha256(content.encode()).hexdigest()
                cursor.execute("SELECT to_regclass('braincore.schema_migrations')")
                ledger_exists = cursor.fetchone()
                existing = None
                if ledger_exists and ledger_exists[0] is not None:
                    cursor.execute(
                        """SELECT checksum FROM braincore.schema_migrations
                           WHERE version = %s""",
                        (path.name,),
                    )
                    existing = cursor.fetchone()
                if existing:
                    if existing[0] != checksum:
                        raise RuntimeError(f"applied migration changed: {path.name}")
                    continue
                # Pin deparser qualification independently of the database role's name.
                # A role named braincore otherwise puts that schema on "$user".
                # pg_catalog remains implicitly first; extensions install in public.
                cursor.execute("SET LOCAL search_path TO public")
                cursor.execute(content)
                cursor.execute(
                    """INSERT INTO braincore.schema_migrations(version, checksum)
                       VALUES (%s, %s) ON CONFLICT (version) DO NOTHING""",
                    (path.name, checksum),
                )
        conn.commit()


def bootstrap_admin() -> None:
    settings = get_settings()
    configured = settings.bootstrap_admin_key
    if configured is None:
        return
    raw = configured.get_secret_value()
    if len(raw) < 32:
        raise RuntimeError("BRAINCORE_BOOTSTRAP_ADMIN_KEY must contain at least 32 characters")
    with psycopg.connect(settings.database_url.get_secret_value(), autocommit=False) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """INSERT INTO braincore.tenants(name) VALUES ('default')
                   ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name RETURNING id"""
            )
            tenant_row = cursor.fetchone()
            if tenant_row is None:
                raise RuntimeError("failed to create or load the bootstrap tenant")
            tenant_id = tenant_row[0]
            digest = key_digest(raw)
            cursor.execute(
                """SELECT id,digest FROM braincore.api_keys
                   WHERE tenant_id=%s AND name='bootstrap'
                   ORDER BY created_at DESC LIMIT 1 FOR UPDATE""",
                (tenant_id,),
            )
            existing = cursor.fetchone()
            if existing is None:
                cursor.execute(
                    """INSERT INTO braincore.api_keys(tenant_id,name,prefix,digest,roles)
                       VALUES (%s,'bootstrap',%s,%s,ARRAY['read','write','admin'])""",
                    (tenant_id, raw[:12], Binary(digest)),
                )
            elif bytes(existing[1]) != digest:
                cursor.execute(
                    """UPDATE braincore.dashboard_sessions
                       SET revoked_at=COALESCE(revoked_at,now())
                       WHERE tenant_id=%s AND api_key_id=%s""",
                    (tenant_id, existing[0]),
                )
                cursor.execute(
                    """UPDATE braincore.api_keys
                       SET prefix=%s,digest=%s,roles=ARRAY['read','write','admin'],scope=NULL,
                           expires_at=NULL,revoked_at=NULL,last_used_at=NULL
                       WHERE tenant_id=%s AND id=%s""",
                    (raw[:12], Binary(digest), tenant_id, existing[0]),
                )
                cursor.execute(
                    """UPDATE braincore.api_keys SET revoked_at=COALESCE(revoked_at,now())
                       WHERE tenant_id=%s AND name='bootstrap' AND id<>%s""",
                    (tenant_id, existing[0]),
                )
        conn.commit()


def main() -> None:
    apply_migrations()
    bootstrap_admin()


if __name__ == "__main__":
    main()
