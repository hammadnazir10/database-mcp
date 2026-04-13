import re
import sys
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg

from config import (
    DATABASE_URL, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD,
    PG_DATABASE, PG_SCHEMA, READ_ONLY, POOL_MIN, POOL_MAX,
)

_pool: Optional[asyncpg.Pool] = None


def get_pool() -> Optional[asyncpg.Pool]:
    return _pool


def check_pool() -> Optional[str]:
    if _pool is None:
        return (
            "Error: No database connection. "
            "Set DATABASE_URL or PG_HOST/PG_PORT/PG_USER/PG_PASSWORD/PG_DATABASE "
            "environment variables and restart the server."
        )
    return None


def build_dsn() -> str:
    if DATABASE_URL:
        return DATABASE_URL
    dsn = f"postgresql://{PG_USER}"
    if PG_PASSWORD:
        dsn += f":{PG_PASSWORD}"
    dsn += f"@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    return dsn


@asynccontextmanager
async def app_lifespan(server):
    global _pool
    dsn = build_dsn()

    safe_dsn = re.sub(r"://([^:]+):([^@]+)@", r"://\1:****@", dsn)
    print(f"Connecting to: {safe_dsn}", file=sys.stderr)

    try:
        _pool = await asyncpg.create_pool(
            dsn,
            min_size=POOL_MIN,
            max_size=POOL_MAX,
            command_timeout=30,
        )
        async with _pool.acquire() as conn:
            version = await conn.fetchval("SELECT version()")
            db_name = await conn.fetchval("SELECT current_database()")
            table_count = await conn.fetchval(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = $1 AND table_type = 'BASE TABLE'",
                PG_SCHEMA,
            )
        print(
            f"Connected to '{db_name}' — {table_count} table(s) in '{PG_SCHEMA}' schema",
            file=sys.stderr,
        )
        print(f"   {version.split(',')[0]}", file=sys.stderr)
        if READ_ONLY:
            print("Read-only mode is ON — write operations are disabled", file=sys.stderr)
    except Exception as e:
        print(f"Connection failed: {e}", file=sys.stderr)
        print(
            "Set DATABASE_URL or PG_HOST/PG_PORT/PG_USER/PG_PASSWORD/PG_DATABASE",
            file=sys.stderr,
        )
        _pool = None

    yield {"dsn": dsn, "read_only": READ_ONLY}

    if _pool:
        await _pool.close()
        print("Connection pool closed.", file=sys.stderr)
