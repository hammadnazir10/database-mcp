import json

from mcp.server.fastmcp import FastMCP

import database
from config import READ_ONLY
from helpers import format_error, is_write_query, record_to_dict, validate_sql_safety
from models.inputs import ExecuteInput

_BLOCKED_OPS = {"DROP", "ALTER", "TRUNCATE", "RENAME", "CREATE", "GRANT", "REVOKE"}


def register(mcp: FastMCP) -> None:
    @mcp.tool(
        name="db_execute",
        annotations={
            "title": "Execute Write SQL Statement",
            "readOnlyHint": False,
            "destructiveHint": True,
            "idempotentHint": False,
            "openWorldHint": False,
        },
    )
    async def db_execute(params: ExecuteInput) -> str:
        """Execute a write SQL statement (INSERT, UPDATE, or DELETE).

        Modifies the database. Blocked when DB_READ_ONLY=true.
        DROP, ALTER, TRUNCATE, CREATE are always blocked for safety.
        """
        err = database.check_pool()
        if err:
            return err

        if READ_ONLY:
            return (
                "Error: Database is in read-only mode. "
                "Write operations are disabled. Set DB_READ_ONLY=false to enable writes."
            )

        pool = database.get_pool()
        sql = params.sql.strip()

        if sql.upper().startswith(("SELECT", "WITH")):
            return "Error: Use db_query for SELECT statements."

        first_word = sql.split()[0].upper() if sql.split() else ""

        if first_word in _BLOCKED_OPS:
            return (
                f"Error: {first_word} operations are blocked for safety. "
                "Only INSERT, UPDATE, and DELETE are allowed."
            )

        if not is_write_query(sql):
            return "Error: Only INSERT, UPDATE, and DELETE are allowed."

        safety_error = validate_sql_safety(sql)
        if safety_error:
            return safety_error

        try:
            async with pool.acquire() as conn:
                if first_word == "INSERT" and "RETURNING" in sql.upper():
                    rows = await conn.fetch(sql)
                    data = [record_to_dict(r) for r in rows]
                    return json.dumps({
                        "status": "success",
                        "operation": "INSERT",
                        "returned_rows": data,
                        "row_count": len(data),
                    }, indent=2, default=str)

                result = await conn.execute(sql)
                parts = result.split()
                affected = int(parts[-1]) if parts[-1].isdigit() else 0

            lines = [f"✅ **{first_word}** executed successfully."]
            lines.append(f"- Rows affected: **{affected}**")
            if first_word == "INSERT":
                lines.append("\n_Tip: Add `RETURNING *` to your INSERT to get the inserted row back._")
            lines.append("_Use `db_query` to verify the changes._")
            return "\n".join(lines)

        except Exception as e:
            return format_error(e)
