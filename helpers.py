import re
from typing import Any, Dict, List, Optional

import asyncpg

from config import DANGEROUS_PATTERNS, WRITE_KEYWORDS


def is_write_query(sql: str) -> bool:
    normalized = sql.strip().upper()
    first_word = normalized.split()[0] if normalized.split() else ""
    return first_word in WRITE_KEYWORDS


def validate_sql_safety(sql: str) -> Optional[str]:
    for pattern in DANGEROUS_PATTERNS:
        if re.search(pattern, sql, re.IGNORECASE):
            return (
                "Error: Query blocked — matched dangerous pattern. "
                "This is a safety measure to prevent SQL injection or "
                "unsafe operations. Please use simple, single-statement queries."
            )
    stripped = sql.strip().rstrip(";")
    if ";" in stripped:
        return (
            "Error: Multiple SQL statements are not allowed. "
            "Please send one query at a time for safety."
        )
    return None


def format_rows_as_markdown(columns: List[str], rows: List[Dict]) -> str:
    if not rows:
        return "_No results found._"

    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join(["---"] * len(columns)) + " |"

    row_lines = []
    for row in rows:
        values = []
        for col in columns:
            val = row.get(col)
            if val is None:
                values.append("NULL")
            else:
                s = str(val)
                if len(s) > 80:
                    s = s[:77] + "..."
                values.append(s)
        row_lines.append("| " + " | ".join(values) + " |")

    return "\n".join([header, separator] + row_lines)


def format_error(e: Exception) -> str:
    msg = str(e)
    if "relation" in msg and "does not exist" in msg:
        return (
            f"Error: {msg}. "
            "Use db_list_tables to see available tables, "
            "then db_describe_table to check the schema."
        )
    if "column" in msg and "does not exist" in msg:
        return (
            f"Error: {msg}. "
            "Use db_describe_table to see the correct column names."
        )
    if "duplicate key" in msg:
        return (
            f"Error: {msg}. "
            "A record with this value already exists. "
            "Use db_query to check existing records first."
        )
    if "foreign key" in msg.lower() or "violates foreign key" in msg:
        return (
            f"Error: {msg}. "
            "The referenced record doesn't exist. "
            "Check the parent table using db_query."
        )
    if "syntax error" in msg.lower():
        return (
            f"Error: SQL syntax error — {msg}. "
            "Please check your SQL syntax and try again."
        )
    if "could not connect" in msg.lower() or "connection" in msg.lower():
        return (
            f"Error: Database connection failed — {msg}. "
            "Check your DATABASE_URL or PG_* environment variables."
        )
    return f"Error: {msg}"


def serialize_value(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (int, float, str, bool)):
        return val
    if isinstance(val, (list, tuple)):
        return [serialize_value(v) for v in val]
    if isinstance(val, dict):
        return {k: serialize_value(v) for k, v in val.items()}
    return str(val)


def record_to_dict(record: asyncpg.Record) -> Dict[str, Any]:
    return {k: serialize_value(v) for k, v in dict(record).items()}
