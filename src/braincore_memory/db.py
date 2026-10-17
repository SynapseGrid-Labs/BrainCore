from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from .config import get_settings

_pool: AsyncConnectionPool[AsyncConnection[dict[str, object]]] | None = None


async def open_pool() -> None:
    global _pool
    if _pool is None:
        _pool = AsyncConnectionPool(
            conninfo=get_settings().database_url.get_secret_value(),
            min_size=1,
            max_size=10,
            kwargs={"row_factory": dict_row, "autocommit": False},
            open=False,
        )
        await _pool.open(wait=True)


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


@asynccontextmanager
async def connection() -> AsyncIterator[AsyncConnection[dict[str, object]]]:
    if _pool is None:
        raise RuntimeError("database pool is not open")
    async with _pool.connection() as conn:
        yield conn
