import json

from mcp.server.fastmcp import FastMCP

import database
from config import PG_SCHEMA
from helpers import format_error, serialize_value
from models.inputs import ResponseFormat, TableStatsInput

_NUMERIC_TYPES = {
    "integer", "bigint", "smallint", "numeric", "decimal",
    "real", "double precision", "serial", "bigserial",
}


def register(mcp: FastMCP) -> None:
    @mcp.tool(
        name="db_table_stats",
        annotations={
            "title": "Get Table Statistics",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def db_table_stats(params: TableStatsInput) -> str:
        """Get summary statistics for a table — row count, NULL counts,
        distinct values, and min/max/avg for numeric columns.
        """
        err = database.check_pool()
        if err:
            return err

        pool = database.get_pool()
        schema = params.schema_name or PG_SCHEMA

        try:
            async with pool.acquire() as conn:
                exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = $1 AND table_name = $2)",
                    schema,
                    params.table_name,
                )
                if not exists:
                    return f"Error: Table '{params.table_name}' not found. Use db_list_tables."

                columns_info = await conn.fetch("""
                    SELECT column_name, data_type, udt_name
                    FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name = $2
                    ORDER BY ordinal_position
                """, schema, params.table_name)

                row_count = await conn.fetchval(
                    f'SELECT COUNT(*) FROM "{schema}"."{params.table_name}"'
                )

                stats = []
                for col in columns_info:
                    col_name = col["column_name"]
                    dtype = col["data_type"]

                    stat = {
                        "column": col_name,
                        "type": dtype,
                        "null_count": await conn.fetchval(
                            f'SELECT COUNT(*) FROM "{schema}"."{params.table_name}" '
                            f'WHERE "{col_name}" IS NULL'
                        ),
                        "distinct_count": await conn.fetchval(
                            f'SELECT COUNT(DISTINCT "{col_name}") FROM "{schema}"."{params.table_name}"'
                        ),
                    }

                    if dtype in _NUMERIC_TYPES:
                        agg = await conn.fetchrow(
                            f'SELECT MIN("{col_name}") AS mn, MAX("{col_name}") AS mx, '
                            f'AVG("{col_name}")::numeric(20,2) AS av '
                            f'FROM "{schema}"."{params.table_name}"'
                        )
                        stat["min"] = serialize_value(agg["mn"])
                        stat["max"] = serialize_value(agg["mx"])
                        stat["avg"] = serialize_value(agg["av"])

                    stats.append(stat)

            if params.response_format == ResponseFormat.JSON:
                return json.dumps({
                    "schema": schema,
                    "table_name": params.table_name,
                    "row_count": row_count,
                    "column_stats": stats,
                }, indent=2, default=str)

            lines = [f"### Stats: `{schema}.{params.table_name}` — {row_count:,} rows\n"]
            lines.append("| Column | Type | Nulls | Distinct | Min | Max | Avg |")
            lines.append("| --- | --- | --- | --- | --- | --- | --- |")
            for s in stats:
                mn = str(s.get("min")) if s.get("min") is not None else "—"
                mx = str(s.get("max")) if s.get("max") is not None else "—"
                av = str(s.get("avg")) if s.get("avg") is not None else "—"
                lines.append(
                    f"| `{s['column']}` | {s['type']} | {s['null_count']} | "
                    f"{s['distinct_count']} | {mn} | {mx} | {av} |"
                )
            return "\n".join(lines)

        except Exception as e:
            return format_error(e)
