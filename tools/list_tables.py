import json

from mcp.server.fastmcp import FastMCP

import database
from config import PG_SCHEMA
from helpers import format_error
from models.inputs import ListTablesInput, ResponseFormat


def register(mcp: FastMCP) -> None:
    @mcp.tool(
        name="db_list_tables",
        annotations={
            "title": "List Database Tables",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def db_list_tables(params: ListTablesInput) -> str:
        """List all tables in a schema with their row counts and descriptions.

        Returns table names, approximate row counts, and table comments.
        Use this as the first step to understand the database structure.
        """
        err = database.check_pool()
        if err:
            return err

        pool = database.get_pool()
        schema = params.schema_name or PG_SCHEMA

        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT
                        t.table_name,
                        pg_catalog.obj_description(c.oid, 'pg_class') AS description,
                        c.reltuples::bigint AS approx_rows
                    FROM information_schema.tables t
                    JOIN pg_catalog.pg_class c ON c.relname = t.table_name
                    JOIN pg_catalog.pg_namespace n
                        ON n.oid = c.relnamespace AND n.nspname = t.table_schema
                    WHERE t.table_schema = $1
                      AND t.table_type = 'BASE TABLE'
                    ORDER BY t.table_name
                """, schema)

            result = [
                {
                    "table_name": r["table_name"],
                    "approx_rows": max(r["approx_rows"], 0),
                    "description": r["description"],
                }
                for r in rows
            ]

            if params.response_format == ResponseFormat.JSON:
                return json.dumps(
                    {"schema": schema, "tables": result, "total_tables": len(result)},
                    indent=2,
                )

            if not result:
                return (
                    f"No tables found in schema `{schema}`. "
                    "Use db_list_schemas to see available schemas."
                )

            lines = [f"**Schema:** `{schema}` — {len(result)} table(s)\n"]
            lines.append("| Table | ~Rows | Description |")
            lines.append("| --- | --- | --- |")
            for t in result:
                desc = t["description"] or "—"
                lines.append(f"| `{t['table_name']}` | {t['approx_rows']:,} | {desc} |")
            lines.append("\n_Use `db_describe_table` to see the schema of any table._")
            return "\n".join(lines)

        except Exception as e:
            return format_error(e)
