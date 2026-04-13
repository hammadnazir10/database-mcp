import json

from mcp.server.fastmcp import FastMCP

import database
from config import PG_DATABASE, PG_SCHEMA
from helpers import format_error
from models.inputs import ListSchemasInput, ResponseFormat


def register(mcp: FastMCP) -> None:
    @mcp.tool(
        name="db_list_schemas",
        annotations={
            "title": "List Database Schemas",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def db_list_schemas(params: ListSchemasInput) -> str:
        """List all schemas in the connected PostgreSQL database.

        Shows every schema with its table count. Use this to discover
        which schemas are available before listing tables.
        """
        err = database.check_pool()
        if err:
            return err

        pool = database.get_pool()

        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT
                        s.schema_name,
                        COUNT(t.table_name) AS table_count
                    FROM information_schema.schemata s
                    LEFT JOIN information_schema.tables t
                        ON t.table_schema = s.schema_name
                        AND t.table_type = 'BASE TABLE'
                    WHERE s.schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                    GROUP BY s.schema_name
                    ORDER BY s.schema_name
                """)

            result = [
                {"schema_name": r["schema_name"], "table_count": r["table_count"]}
                for r in rows
            ]

            if params.response_format == ResponseFormat.JSON:
                return json.dumps({"schemas": result}, indent=2)

            if not result:
                return "No user schemas found."

            lines = [f"**Database:** `{PG_DATABASE}` — {len(result)} schema(s)\n"]
            lines.append("| Schema | Tables |")
            lines.append("| --- | --- |")
            for s in result:
                marker = " ← active" if s["schema_name"] == PG_SCHEMA else ""
                lines.append(f"| `{s['schema_name']}` | {s['table_count']} |{marker}")
            return "\n".join(lines)

        except Exception as e:
            return format_error(e)
