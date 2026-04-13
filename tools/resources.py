import json

from mcp.server.fastmcp import FastMCP

import database
from config import PG_DATABASE, PG_HOST, PG_PORT, PG_SCHEMA, PG_USER, MAX_ROWS, POOL_MIN, POOL_MAX, READ_ONLY


def register(mcp: FastMCP) -> None:
    @mcp.resource("db://info")
    async def db_info() -> str:
        """Provides connection and configuration info about the database."""
        pool = database.get_pool()

        info = {
            "host": PG_HOST,
            "port": PG_PORT,
            "database": PG_DATABASE,
            "schema": PG_SCHEMA,
            "user": PG_USER,
            "read_only_mode": READ_ONLY,
            "max_rows_per_query": MAX_ROWS,
            "pool_size": f"{POOL_MIN}-{POOL_MAX}",
            "connected": pool is not None,
        }

        if pool:
            try:
                async with pool.acquire() as conn:
                    info["server_version"] = str(conn.get_server_version())
                    info["current_database"] = await conn.fetchval("SELECT current_database()")
            except Exception as e:
                info["error"] = str(e)

        return json.dumps(info, indent=2)
