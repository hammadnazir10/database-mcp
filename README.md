# 🐘 Database Query Assistant — PostgreSQL MCP Server

A production-quality MCP server that lets AI agents interact with **any PostgreSQL database**. Just point it at your database using a connection string, and Claude / Cursor / Claude Code can explore schemas, run queries, and modify data — all with built-in safety.

> **Connect YOUR database** — works with local Postgres, Supabase, Neon, Railway, AWS RDS, or any PostgreSQL instance.

---

## ✨ Features

| Tool | Description | Read-Only? |
|------|-------------|------------|
| `db_list_schemas` | List all schemas with table counts | ✅ |
| `db_list_tables` | List tables with row counts and descriptions | ✅ |
| `db_describe_table` | Full schema — columns, types, FKs, indexes, comments | ✅ |
| `db_query` | Execute SELECT queries (with CTEs, JOINs, window funcs) | ✅ |
| `db_execute` | Run INSERT / UPDATE / DELETE statements | ❌ |
| `db_search` | Case-insensitive search across all text columns | ✅ |
| `db_table_stats` | Column stats — nulls, distinct, min/max/avg | ✅ |

### Safety Features

- **Read-only mode** — set `DB_READ_ONLY=true` to block all writes
- **Read-only transactions** — SELECT queries run inside `readonly=True` transactions
- **SQL injection protection** — blocks comments, chained statements, dangerous patterns
- **Auto-limiting** — queries capped at 100 rows max
- **Destructive blocking** — DROP, ALTER, TRUNCATE, CREATE always blocked
- **Connection pooling** — asyncpg pool (2-10 connections by default)
- **Input validation** — Pydantic models validate every parameter

---

## 📁 Project Structure

```
mcp-db/
├── server.py              # Entry point — wires MCP + registers all tools
├── config.py              # All env vars and safety constants
├── database.py            # Connection pool lifecycle (startup / shutdown)
├── helpers.py             # Shared utilities: formatting, validation, serialization
├── models/
│   ├── __init__.py
│   └── inputs.py          # Pydantic input models for all tools
├── tools/
│   ├── __init__.py
│   ├── list_schemas.py    # db_list_schemas
│   ├── list_tables.py     # db_list_tables
│   ├── describe_table.py  # db_describe_table
│   ├── query.py           # db_query
│   ├── execute.py         # db_execute
│   ├── search.py          # db_search
│   ├── stats.py           # db_table_stats
│   └── resources.py       # db://info MCP resource
└── requirements.txt
```

**Each tool lives in its own file** and exposes a `register(mcp)` function. `server.py` calls each one at startup — adding or removing a tool only requires one line.

---

## 🚀 Quick Start

### 1. Clone and install dependencies

```bash
git clone https://github.com/hammadnazir10/database-mcp.git
cd database-mcp
pip install -r requirements.txt
```

### 2. Set your database connection

**Option A — Connection URL (recommended):**
```bash
export DATABASE_URL="postgresql://myuser:mypassword@localhost:5432/mydb"
```

**Option B — Individual variables:**
```bash
export PG_HOST="localhost"
export PG_PORT="5432"
export PG_USER="myuser"
export PG_PASSWORD="mypassword"
export PG_DATABASE="mydb"
export PG_SCHEMA="public"        # optional, defaults to 'public'
```

**Cloud databases:**
```bash
# Supabase
export DATABASE_URL="postgresql://postgres.xxxx:password@aws-0-ap-south-1.pooler.supabase.com:6543/postgres"

# Neon
export DATABASE_URL="postgresql://user:pass@ep-xxx.ap-southeast-1.aws.neon.tech/mydb?sslmode=require"

# Railway
export DATABASE_URL="postgresql://postgres:pass@roundhouse.proxy.rlwy.net:port/railway"
```

### 3. Run the server

```bash
# Local (stdio — for Claude Desktop, Cursor, Claude Code)
python server.py

# Remote (HTTP — for multi-client setups)
python server.py --transport http --port 8000
```

---

## 🔌 Connect to AI Clients

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (Mac) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "my-database": {
      "command": "python",
      "args": ["/full/path/to/server.py"],
      "env": {
        "DATABASE_URL": "postgresql://user:pass@localhost:5432/mydb",
        "DB_READ_ONLY": "false"
      }
    }
  }
}
```

### Claude Code (VS Code Extension)

Add to `.vscode/mcp.json` in your project:

```json
{
  "servers": {
    "my-database": {
      "command": "python",
      "args": ["/full/path/to/server.py"],
      "env": {
        "DATABASE_URL": "postgresql://user:pass@localhost:5432/mydb"
      }
    }
  }
}
```

### Cursor IDE

Go to **Settings → MCP Servers → Add** and paste the config above.

### MCP Inspector (for testing)

```bash
DATABASE_URL="postgresql://user:pass@localhost:5432/mydb" \
  npx @modelcontextprotocol/inspector python server.py
```

---

## ⚙️ Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | — | Full PostgreSQL connection URL (takes priority over PG_* vars) |
| `PG_HOST` | `localhost` | PostgreSQL host |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_USER` | `postgres` | Database user |
| `PG_PASSWORD` | — | Database password |
| `PG_DATABASE` | `postgres` | Database name |
| `PG_SCHEMA` | `public` | Default schema for all tools |
| `DB_READ_ONLY` | `false` | Set to `true` to block all write operations |
| `DB_MAX_ROWS` | `100` | Maximum rows returned per query |
| `DB_POOL_MIN` | `2` | Minimum connection pool size |
| `DB_POOL_MAX` | `10` | Maximum connection pool size |

---

## 🧪 Example Conversations

Once connected, try these prompts with Claude:

```
"What schemas are in this database?"
→ Claude calls db_list_schemas

"Show me all tables"
→ Claude calls db_list_tables

"Describe the users table"
→ Claude calls db_describe_table

"Find all orders placed in March 2025 with total > 50000"
→ Claude calls db_query with appropriate SQL

"Search for 'Ahmed' in the customers table"
→ Claude calls db_search

"Insert a new product: name='Widget', price=999, stock=50"
→ Claude calls db_execute

"Give me stats on the orders table"
→ Claude calls db_table_stats
```

---

## 🔒 Security Best Practices

1. **Use a dedicated database user** with limited permissions
2. **Enable read-only mode** (`DB_READ_ONLY=true`) for exploration
3. **Never commit** your `DATABASE_URL` — use env vars or `.env` files
4. **Use SSL** for cloud databases (`?sslmode=require` in URL)
5. **Limit pool size** to avoid overwhelming your database

```sql
-- Create a limited user for the MCP server
CREATE USER mcp_agent WITH PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE mydb TO mcp_agent;
GRANT USAGE ON SCHEMA public TO mcp_agent;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO mcp_agent;
-- Add INSERT/UPDATE/DELETE only if needed:
-- GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO mcp_agent;
```

---

## 📄 License

MIT — use freely for learning, portfolio, and production.
