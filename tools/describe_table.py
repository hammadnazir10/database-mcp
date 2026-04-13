import json

from mcp.server.fastmcp import FastMCP

import database
from config import PG_SCHEMA
from helpers import format_error
from models.inputs import DescribeTableInput, ResponseFormat


def register(mcp: FastMCP) -> None:
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
                    return (
                        f"Error: Table '{params.table_name}' not found in schema '{schema}'. "
                        "Use db_list_tables to see available tables."
                    )

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

                pk_cols = await conn.fetch("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    JOIN pg_class c ON c.oid = i.indrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = $1 AND n.nspname = $2 AND i.indisprimary
                """, params.table_name, schema)
                pk_set = {r["attname"] for r in pk_cols}

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
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage ccu
                        ON ccu.constraint_name = tc.constraint_name
                        AND ccu.table_schema = tc.table_schema
                    JOIN information_schema.referential_constraints rc
                        ON rc.constraint_name = tc.constraint_name
                        AND rc.constraint_schema = tc.table_schema
                    WHERE tc.table_schema = $1
                      AND tc.table_name = $2
                      AND tc.constraint_type = 'FOREIGN KEY'
                """, schema, params.table_name)

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

                row_count = await conn.fetchval(
                    f'SELECT COUNT(*) FROM "{schema}"."{params.table_name}"'
                )

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

            lines = [f"### Table: `{schema}.{params.table_name}` ({row_count:,} rows)\n"]
            lines.append("| Column | Type | Nullable | Default | PK | Comment |")
            lines.append("| --- | --- | --- | --- | --- | --- |")
            for c in col_data:
                pk = "✅" if c["primary_key"] else ""
                nullable = "Yes" if c["nullable"] else "No"
                default = str(c["default"])[:30] if c["default"] else "—"
                comment = c["comment"] or "—"
                lines.append(
                    f"| `{c['name']}` | {c['type']} | {nullable} | {default} | {pk} | {comment} |"
                )

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
