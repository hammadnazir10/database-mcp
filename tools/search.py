import json

from mcp.server.fastmcp import FastMCP

import database
from config import PG_SCHEMA
from helpers import format_error, format_rows_as_markdown, record_to_dict
from models.inputs import ResponseFormat, SearchInput


def register(mcp: FastMCP) -> None:
    @mcp.tool(
        name="db_search",
        annotations={
            "title": "Search Table for Text",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def db_search(params: SearchInput) -> str:
        """Search for a text term across all text columns in a table.

        Performs case-insensitive ILIKE search across every text, varchar,
        and char column. Useful when you don't know which column to search.
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

                text_cols = await conn.fetch("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name = $2
                      AND data_type IN (
                          'text', 'character varying', 'character',
                          'varchar', 'char', 'name'
                      )
                    ORDER BY ordinal_position
                """, schema, params.table_name)

                col_names = [c["column_name"] for c in text_cols]
                if not col_names:
                    return f"No searchable text columns found in '{params.table_name}'."

                conditions = " OR ".join([f'"{col}"::text ILIKE $1' for col in col_names])
                search_pattern = f"%{params.search_term}%"
                limit = params.limit or 20

                async with conn.transaction(readonly=True):
                    rows = await conn.fetch(
                        f'SELECT * FROM "{schema}"."{params.table_name}" '
                        f"WHERE {conditions} LIMIT {limit}",
                        search_pattern,
                    )

            columns = list(rows[0].keys()) if rows else []
            data = [record_to_dict(r) for r in rows]

            if params.response_format == ResponseFormat.JSON:
                return json.dumps({
                    "table": params.table_name,
                    "search_term": params.search_term,
                    "searched_columns": col_names,
                    "rows": data,
                    "row_count": len(data),
                }, indent=2, default=str)

            header = (
                f"**Search:** '{params.search_term}' in `{params.table_name}` "
                f"(columns: {', '.join(col_names)})\n"
                f"**Found:** {len(data)} result(s)\n"
            )
            return header + format_rows_as_markdown(columns, data)

        except Exception as e:
            return format_error(e)
