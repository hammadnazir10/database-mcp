import asyncio
import json
import os
import re
import sys
from contextlib import asynccontextmanager
from enum import Enum
from typing import Any, Dict, List, Optional

import asyncpg
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, ConfigDict, Field, field_validator

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

# Users connect their own DB via env vars
# Priority: DATABASE_URL > individual PG_* vars
DATABASE_URL = os.environ.get("DATABASE_URL", "")
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "")
PG_DATABASE = os.environ.get("PG_DATABASE", "postgres")
PG_SCHEMA = os.environ.get("PG_SCHEMA", "public")

READ_ONLY = os.environ.get("DB_READ_ONLY", "false").lower() == "true"
MAX_ROWS = int(os.environ.get("DB_MAX_ROWS", "100"))
POOL_MIN = int(os.environ.get("DB_POOL_MIN", "2"))
POOL_MAX = int(os.environ.get("DB_POOL_MAX", "10"))

# SQL keywords that indicate write operations
WRITE_KEYWORDS = {
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
    "REPLACE", "TRUNCATE", "RENAME", "GRANT", "REVOKE",
}

# Dangerous patterns to block
DANGEROUS_PATTERNS = [
    r";\s*(DROP|DELETE|ALTER|TRUNCATE|GRANT|REVOKE)",
    r"--",
    r"/\*",
    r"\bCOPY\b",
    r"\bEXECUTE\b",
    r"\bPREPARE\b",
    r"\bDEALLOCATE\b",
    r"\bLISTEN\b",
    r"\bNOTIFY\b",
    r"\bLOAD\b",
    r"\bDO\s+\$",
]


def build_dsn() -> str:
    """Build the PostgreSQL connection string from env vars."""
    if DATABASE_URL:
        return DATABASE_URL
    dsn = f"postgresql://{PG_USER}"
    if PG_PASSWORD:
        dsn += f":{PG_PASSWORD}"
    dsn += f"@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    return dsn


# ─────────────────────────────────────────────
# Global connection pool (set in lifespan)
# ─────────────────────────────────────────────

pool: Optional[asyncpg.Pool] = None


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def is_write_query(sql: str) -> bool:
    """Check if a SQL statement is a write operation."""
    normalized = sql.strip().upper()
    first_word = normalized.split()[0] if normalized.split() else ""
    return first_word in WRITE_KEYWORDS


def validate_sql_safety(sql: str) -> Optional[str]:
    """
    Validate SQL for dangerous patterns.
    Returns an error message if unsafe, None if safe.
    """
    for pattern in DANGEROUS_PATTERNS:
        if re.search(pattern, sql, re.IGNORECASE):
            return (
                f"Error: Query blocked — matched dangerous pattern. "
                f"This is a safety measure to prevent SQL injection or "
                f"unsafe operations. Please use simple, single-statement queries."
            )
    # Check for multiple statements (semicolon not at the end)
    stripped = sql.strip().rstrip(";")
    if ";" in stripped:
        return (
            "Error: Multiple SQL statements are not allowed. "
            "Please send one query at a time for safety."
        )
    return None


def format_rows_as_markdown(columns: List[str], rows: List[Dict]) -> str:
    """Format query results as a markdown table."""
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
                # Truncate very long values in markdown
                if len(s) > 80:
                    s = s[:77] + "..."
                values.append(s)
        row_lines.append("| " + " | ".join(values) + " |")

    return "\n".join([header, separator] + row_lines)


def format_error(e: Exception) -> str:
    """Format database errors into actionable messages."""
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
    """Convert PostgreSQL types to JSON-serializable values."""
    if val is None:
        return None
    if isinstance(val, (int, float, str, bool)):
        return val
    if isinstance(val, (list, tuple)):
        return [serialize_value(v) for v in val]
    if isinstance(val, dict):
        return {k: serialize_value(v) for k, v in val.items()}
    # datetime, date, Decimal, UUID, etc.
    return str(val)


def record_to_dict(record: asyncpg.Record) -> Dict[str, Any]:
    """Convert asyncpg Record to a serializable dict."""
    return {k: serialize_value(v) for k, v in dict(record).items()}


# ─────────────────────────────────────────────
# Response format enum
# ─────────────────────────────────────────────

class ResponseFormat(str, Enum):
    """Output format for tool responses."""
    MARKDOWN = "markdown"
    JSON = "json"


# ─────────────────────────────────────────────
# Pydantic input models
# ─────────────────────────────────────────────

class ListTablesInput(BaseModel):
    """Input for listing database tables."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    schema_name: Optional[str] = Field(
        default=None,
        description="PostgreSQL schema to list tables from (defaults to PG_SCHEMA env var, usually 'public')",
        max_length=128,
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' for human-readable or 'json' for structured data"
    )


class DescribeTableInput(BaseModel):
    """Input for describing a table's schema."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    table_name: str = Field(
        ...,
        description="Name of the table to describe (e.g., 'users', 'orders')",
        min_length=1,
        max_length=128,
    )
    schema_name: Optional[str] = Field(
        default=None,
        description="Schema name (defaults to PG_SCHEMA, usually 'public')",
        max_length=128,
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'"
    )

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError(
                "Table name must start with a letter or underscore and contain "
                "only letters, numbers, and underscores."
            )
        return v


class QueryInput(BaseModel):
    """Input for executing a SELECT query."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    sql: str = Field(
        ...,
        description=(
            "SQL SELECT query to execute. Only read-only queries allowed. "
            "Examples: 'SELECT * FROM users WHERE age > 25', "
            "'SELECT name, COUNT(*) FROM orders GROUP BY name'"
        ),
        min_length=1,
        max_length=5000,
    )
    limit: Optional[int] = Field(
        default=50,
        description="Maximum rows to return (1-100). Defaults to 50.",
        ge=1,
        le=100,
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' for table or 'json' for structured data"
    )


class ExecuteInput(BaseModel):
    """Input for executing a write operation (INSERT/UPDATE/DELETE)."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    sql: str = Field(
        ...,
        description=(
            "SQL statement to execute (INSERT, UPDATE, or DELETE). "
            "Examples: \"INSERT INTO users (name, email) VALUES ('Ali', 'ali@test.com')\", "
            "\"UPDATE users SET age = 30 WHERE id = 1\", "
            "\"DELETE FROM users WHERE id = 99\""
        ),
        min_length=1,
        max_length=5000,
    )


class SearchInput(BaseModel):
    """Input for searching across tables."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    search_term: str = Field(
        ...,
        description="Text to search for across all text columns in the specified table",
        min_length=1,
        max_length=200,
    )
    table_name: str = Field(
        ...,
        description="Table to search in (e.g., 'users', 'products')",
        min_length=1,
        max_length=128,
    )
    schema_name: Optional[str] = Field(
        default=None,
        description="Schema name (defaults to PG_SCHEMA)",
        max_length=128,
    )
    limit: Optional[int] = Field(
        default=20,
        description="Maximum rows to return (1-100)",
        ge=1,
        le=100,
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'"
    )

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError("Invalid table name format.")
        return v


class TableStatsInput(BaseModel):
    """Input for getting table statistics."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    table_name: str = Field(
        ...,
        description="Table to get statistics for",
        min_length=1,
        max_length=128,
    )
    schema_name: Optional[str] = Field(
        default=None,
        description="Schema name (defaults to PG_SCHEMA)",
        max_length=128,
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'"
    )

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError("Invalid table name format.")
        return v


class ListSchemasInput(BaseModel):
    """Input for listing database schemas."""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'"
    )


# ─────────────────────────────────────────────
# Lifespan — connection pool setup/teardown
# ─────────────────────────────────────────────

@asynccontextmanager
async def app_lifespan(server):
    """Create a connection pool on startup, close on shutdown."""
    global pool
    dsn = build_dsn()

    # Mask password for logging
    safe_dsn = re.sub(r"://([^:]+):([^@]+)@", r"://\1:****@", dsn)
    print(f"🔌 Connecting to: {safe_dsn}", file=sys.stderr)

    try:
        pool = await asyncpg.create_pool(
            dsn,
            min_size=POOL_MIN,
            max_size=POOL_MAX,
            command_timeout=30,
        )
        async with pool.acquire() as conn:
            version = await conn.fetchval("SELECT version()")
            db_name = await conn.fetchval("SELECT current_database()")
            table_count = await conn.fetchval(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = $1 AND table_type = 'BASE TABLE'",
                PG_SCHEMA,
            )
        print(f"✅ Connected to '{db_name}' — {table_count} table(s) in '{PG_SCHEMA}' schema", file=sys.stderr)
        print(f"   {version.split(',')[0]}", file=sys.stderr)
        if READ_ONLY:
            print("🔒 Read-only mode is ON — write operations are disabled", file=sys.stderr)
    except Exception as e:
        print(f"❌ Connection failed: {e}", file=sys.stderr)
        print(f"   Set DATABASE_URL or PG_HOST/PG_PORT/PG_USER/PG_PASSWORD/PG_DATABASE", file=sys.stderr)
        pool = None

    yield {"dsn": dsn, "read_only": READ_ONLY}

    if pool:
        await pool.close()
        print("🔌 Connection pool closed.", file=sys.stderr)


# ─────────────────────────────────────────────
# MCP Server
# ─────────────────────────────────────────────

mcp = FastMCP("database_mcp", lifespan=app_lifespan)


def _check_pool() -> Optional[str]:
    """Return an error message if pool is not available."""
    if pool is None:
        return (
            "Error: No database connection. "
            "Set DATABASE_URL or PG_HOST/PG_PORT/PG_USER/PG_PASSWORD/PG_DATABASE "
            "environment variables and restart the server."
        )
    return None


# ── Tool 1: List Schemas ─────────────────────

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

    Args:
        params (ListSchemasInput): Contains response_format.

    Returns:
        str: Schema listing in markdown or JSON.
    """
    err = _check_pool()
    if err:
        return err

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

        result = [{"schema_name": r["schema_name"], "table_count": r["table_count"]} for r in rows]

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


# ── Tool 2: List Tables ──────────────────────

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

    Args:
        params (ListTablesInput): Contains schema_name and response_format.

    Returns:
        str: Table listing in markdown or JSON.
    """
    err = _check_pool()
    if err:
        return err

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
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
                WHERE t.table_schema = $1
                  AND t.table_type = 'BASE TABLE'
                ORDER BY t.table_name
            """, schema)

        result = []
        for r in rows:
            approx = max(r["approx_rows"], 0)
            result.append({
                "table_name": r["table_name"],
                "approx_rows": approx,
                "description": r["description"],
            })

        if params.response_format == ResponseFormat.JSON:
            return json.dumps({"schema": schema, "tables": result, "total_tables": len(result)}, indent=2)

        if not result:
            return f"No tables found in schema `{schema}`. Use db_list_schemas to see available schemas."

        lines = [f"**Schema:** `{schema}` — {len(result)} table(s)\n"]
        lines.append("| Table | ~Rows | Description |")
        lines.append("| --- | --- | --- |")
        for t in result:
            desc = t["description"] or "—"
            lines.append(f"| `{t['table_name']}` | {t['approx_rows']:,} | {desc} |")
        lines.append(f"\n_Use `db_describe_table` to see the schema of any table._")
        return "\n".join(lines)

    except Exception as e:
        return format_error(e)


# ── Tool 3: Describe Table ───────────────────

@mcp.tool(
    name="db_describe_table",
    annotations={
        "title": "Describe Table Schema",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def db_describe_table(params: DescribeTableInput) -> str:
    """Describe the schema of a specific table — columns, types, constraints,
    foreign keys, indexes, and table comment.

    Args:
        params (DescribeTableInput): Contains table_name, schema_name, response_format.

    Returns:
        str: Full schema description in markdown or JSON.
    """
    err = _check_pool()
    if err:
        return err

    schema = params.schema_name or PG_SCHEMA

    try:
        async with pool.acquire() as conn:

            # Check table exists
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = $1 AND table_name = $2)",
                schema, params.table_name,
            )
            if not exists:
                return (
                    f"Error: Table '{params.table_name}' not found in schema '{schema}'. "
                    f"Use db_list_tables to see available tables."
                )

            # Columns
            columns = await conn.fetch("""
                SELECT
                    c.column_name,
                    c.data_type,
                    c.udt_name,
                    c.is_nullable,
                    c.column_default,
                    c.character_maximum_length,
                    pgd.description AS comment
                FROM information_schema.columns c
                LEFT JOIN pg_catalog.pg_statio_all_tables st
                    ON st.schemaname = c.table_schema AND st.relname = c.table_name
                LEFT JOIN pg_catalog.pg_description pgd
                    ON pgd.objoid = st.relid AND pgd.objsubid = c.ordinal_position
                WHERE c.table_schema = $1 AND c.table_name = $2
                ORDER BY c.ordinal_position
            """, schema, params.table_name)

            # Primary key columns
            pk_cols = await conn.fetch("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = $1 AND n.nspname = $2 AND i.indisprimary
            """, params.table_name, schema)
            pk_set = {r["attname"] for r in pk_cols}

            # Foreign keys
            fkeys = await conn.fetch("""
                SELECT
                    kcu.column_name AS from_column,
                    ccu.table_schema AS to_schema,
                    ccu.table_name AS to_table,
                    ccu.column_name AS to_column,
                    rc.update_rule,
                    rc.delete_rule
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                    ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
                JOIN information_schema.referential_constraints rc
                    ON rc.constraint_name = tc.constraint_name AND rc.constraint_schema = tc.table_schema
                WHERE tc.table_schema = $1
                  AND tc.table_name = $2
                  AND tc.constraint_type = 'FOREIGN KEY'
            """, schema, params.table_name)

            # Indexes
            indexes = await conn.fetch("""
                SELECT
                    i.relname AS index_name,
                    ix.indisunique AS is_unique,
                    array_agg(a.attname ORDER BY k.n) AS columns
                FROM pg_index ix
                JOIN pg_class t ON t.oid = ix.indrelid
                JOIN pg_class i ON i.oid = ix.indexrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, n)
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
                WHERE t.relname = $1 AND n.nspname = $2 AND NOT ix.indisprimary
                GROUP BY i.relname, ix.indisunique
                ORDER BY i.relname
            """, params.table_name, schema)

            # Row count
            row_count = await conn.fetchval(
                f'SELECT COUNT(*) FROM "{schema}"."{params.table_name}"'
            )

        # Build response
        col_data = []
        for c in columns:
            dtype = c["data_type"]
            if c["character_maximum_length"]:
                dtype += f"({c['character_maximum_length']})"
            col_data.append({
                "name": c["column_name"],
                "type": dtype,
                "pg_type": c["udt_name"],
                "nullable": c["is_nullable"] == "YES",
                "default": c["column_default"],
                "primary_key": c["column_name"] in pk_set,
                "comment": c["comment"],
            })

        fk_data = [
            {
                "from_column": f["from_column"],
                "to_table": f"{f['to_schema']}.{f['to_table']}",
                "to_column": f["to_column"],
                "on_update": f["update_rule"],
                "on_delete": f["delete_rule"],
            }
            for f in fkeys
        ]

        idx_data = [
            {"name": idx["index_name"], "unique": idx["is_unique"], "columns": list(idx["columns"])}
            for idx in indexes
        ]

        if params.response_format == ResponseFormat.JSON:
            return json.dumps({
                "schema": schema,
                "table_name": params.table_name,
                "row_count": row_count,
                "columns": col_data,
                "foreign_keys": fk_data,
                "indexes": idx_data,
            }, indent=2)

        # Markdown
        lines = [f"### Table: `{schema}.{params.table_name}` ({row_count:,} rows)\n"]
        lines.append("| Column | Type | Nullable | Default | PK | Comment |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for c in col_data:
            pk = "✅" if c["primary_key"] else ""
            nullable = "Yes" if c["nullable"] else "No"
            default = str(c["default"])[:30] if c["default"] else "—"
            comment = c["comment"] or "—"
            lines.append(f"| `{c['name']}` | {c['type']} | {nullable} | {default} | {pk} | {comment} |")

        if fk_data:
            lines.append("\n**Foreign keys:**")
            for fk in fk_data:
                lines.append(
                    f"- `{fk['from_column']}` → `{fk['to_table']}.{fk['to_column']}` "
                    f"(ON DELETE {fk['on_delete']})"
                )

        if idx_data:
            lines.append("\n**Indexes:**")
            for idx in idx_data:
                unique = " (unique)" if idx["unique"] else ""
                cols = ", ".join(idx["columns"])
                lines.append(f"- `{idx['name']}`{unique} on ({cols})")

        return "\n".join(lines)

    except Exception as e:
        return format_error(e)


# ── Tool 4: Query (Read-Only) ────────────────

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
    Supports JOINs, GROUP BY, ORDER BY, subqueries, CTEs, window functions,
    and all standard PostgreSQL SELECT features.

    Args:
        params (QueryInput): Contains sql, limit, response_format.

    Returns:
        str: Query results as markdown table or JSON array.
    """
    err = _check_pool()
    if err:
        return err

    sql = params.sql.strip()

    # Must start with SELECT or WITH (for CTEs)
    upper = sql.upper()
    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        return (
            "Error: Only SELECT queries (including WITH/CTE) are allowed with db_query. "
            "Use db_execute for INSERT, UPDATE, or DELETE."
        )

    # Safety check
    safety_error = validate_sql_safety(sql)
    if safety_error:
        return safety_error

    try:
        effective_limit = min(params.limit or 50, MAX_ROWS)

        # Add LIMIT if not present
        if "LIMIT" not in upper:
            sql = f"{sql.rstrip(';')} LIMIT {effective_limit}"

        async with pool.acquire() as conn:
            # Use a read-only transaction for extra safety
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

        header = f"**Query:** `{params.sql.strip()}`\n**Results:** {len(data)} row(s)\n"
        table = format_rows_as_markdown(columns, data)
        return header + table

    except Exception as e:
        return format_error(e)


# ── Tool 5: Execute (Write Operations) ───────

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

    Args:
        params (ExecuteInput): Contains the SQL statement.

    Returns:
        str: Confirmation with affected row count.
    """
    err = _check_pool()
    if err:
        return err

    if READ_ONLY:
        return (
            "Error: Database is in read-only mode. "
            "Write operations are disabled. Set DB_READ_ONLY=false to enable writes."
        )

    sql = params.sql.strip()

    if sql.upper().startswith(("SELECT", "WITH")):
        return "Error: Use db_query for SELECT statements."

    first_word = sql.split()[0].upper() if sql.split() else ""
    blocked = {"DROP", "ALTER", "TRUNCATE", "RENAME", "CREATE", "GRANT", "REVOKE"}
    if first_word in blocked:
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
            # For INSERT with RETURNING, use fetch
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
            # result is like "INSERT 0 1" or "UPDATE 3" or "DELETE 2"
            parts = result.split()
            affected = int(parts[-1]) if parts[-1].isdigit() else 0

        lines = [f"✅ **{first_word}** executed successfully."]
        lines.append(f"- Rows affected: **{affected}**")
        if first_word == "INSERT":
            lines.append(f"\n_Tip: Add `RETURNING *` to your INSERT to get the inserted row back._")
        lines.append(f"_Use `db_query` to verify the changes._")
        return "\n".join(lines)

    except Exception as e:
        return format_error(e)


# ── Tool 6: Search ───────────────────────────

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

    Args:
        params (SearchInput): Contains search_term, table_name, schema_name, limit, response_format.

    Returns:
        str: Matching rows as markdown table or JSON.
    """
    err = _check_pool()
    if err:
        return err

    schema = params.schema_name or PG_SCHEMA

    try:
        async with pool.acquire() as conn:
            # Check table exists
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = $1 AND table_name = $2)",
                schema, params.table_name,
            )
            if not exists:
                return f"Error: Table '{params.table_name}' not found. Use db_list_tables."

            # Get text columns
            text_cols = await conn.fetch("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                  AND data_type IN ('text', 'character varying', 'character', 'varchar', 'char', 'name')
                ORDER BY ordinal_position
            """, schema, params.table_name)

            col_names = [c["column_name"] for c in text_cols]
            if not col_names:
                return f"No searchable text columns found in '{params.table_name}'."

            # Build ILIKE conditions
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
        table = format_rows_as_markdown(columns, data)
        return header + table

    except Exception as e:
        return format_error(e)


# ── Tool 7: Table Statistics ─────────────────

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

    Args:
        params (TableStatsInput): Contains table_name, schema_name, response_format.

    Returns:
        str: Statistics in markdown or JSON.
    """
    err = _check_pool()
    if err:
        return err

    schema = params.schema_name or PG_SCHEMA

    try:
        async with pool.acquire() as conn:
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = $1 AND table_name = $2)",
                schema, params.table_name,
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
                    "null_count": 0,
                    "distinct_count": 0,
                }

                stat["null_count"] = await conn.fetchval(
                    f'SELECT COUNT(*) FROM "{schema}"."{params.table_name}" '
                    f'WHERE "{col_name}" IS NULL'
                )

                stat["distinct_count"] = await conn.fetchval(
                    f'SELECT COUNT(DISTINCT "{col_name}") FROM "{schema}"."{params.table_name}"'
                )

                # Numeric stats
                numeric_types = {
                    "integer", "bigint", "smallint", "numeric", "decimal",
                    "real", "double precision", "serial", "bigserial",
                }
                if dtype in numeric_types:
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
            mn = str(s.get("min", "—")) if s.get("min") is not None else "—"
            mx = str(s.get("max", "—")) if s.get("max") is not None else "—"
            av = str(s.get("avg", "—")) if s.get("avg") is not None else "—"
            lines.append(
                f"| `{s['column']}` | {s['type']} | {s['null_count']} | "
                f"{s['distinct_count']} | {mn} | {mx} | {av} |"
            )
        return "\n".join(lines)

    except Exception as e:
        return format_error(e)


# ── Resource: Connection Info ────────────────

@mcp.resource("db://info")
async def db_info() -> str:
    """Provides connection and configuration info about the database."""
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


# ─────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Database Query Assistant MCP Server (PostgreSQL)")
    parser.add_argument("--transport", choices=["stdio", "http"], default="stdio",
                        help="Transport: stdio (local) or http (remote)")
    parser.add_argument("--port", type=int, default=8000,
                        help="Port for HTTP transport (default: 8000)")
    args = parser.parse_args()

    if args.transport == "http":
        mcp.run(transport="streamable_http", port=args.port)
    else:
        mcp.run()
