import json

from mcp.server.fastmcp import FastMCP

import database
from config import MAX_ROWS
from helpers import format_error, format_rows_as_markdown, record_to_dict, validate_sql_safety
from models.inputs import QueryInput, ResponseFormat


def register(mcp: FastMCP) -> None:
    @mcp.tool(
        name="db_query",
        annotations={
            "title": "Execute Read-Only SQL Query",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def db_query(params: QueryInput) -> str:
        """Execute a read-only SQL SELECT query and return results.

        Only SELECT statements are allowed. Results are automatically limited.
        Supports JOINs, GROUP BY, ORDER BY, subqueries, CTEs, and window functions.
        """
        err = database.check_pool()
        if err:
            return err

        pool = database.get_pool()
        sql = params.sql.strip()
        upper = sql.upper()

        if not (upper.startswith("SELECT") or upper.startswith("WITH")):
            return (
                "Error: Only SELECT queries (including WITH/CTE) are allowed with db_query. "
                "Use db_execute for INSERT, UPDATE, or DELETE."
            )

        safety_error = validate_sql_safety(sql)
        if safety_error:
            return safety_error

        try:
            effective_limit = min(params.limit or 50, MAX_ROWS)

            if "LIMIT" not in upper:
                sql = f"{sql.rstrip(';')} LIMIT {effective_limit}"

            async with pool.acquire() as conn:
                async with conn.transaction(readonly=True):
                    rows = await conn.fetch(sql)

            columns = list(rows[0].keys()) if rows else []
            data = [record_to_dict(r) for r in rows]

            if params.response_format == ResponseFormat.JSON:
                return json.dumps({
                    "columns": columns,
                    "rows": data,
                    "row_count": len(data),
                    "query": params.sql.strip(),
                }, indent=2, default=str)

            header = (
                f"**Query:** `{params.sql.strip()}`\n"
                f"**Results:** {len(data)} row(s)\n"
            )
            return header + format_rows_as_markdown(columns, data)

        except Exception as e:
            return format_error(e)
