import argparse

from mcp.server.fastmcp import FastMCP

from database import app_lifespan
from tools import describe_table, execute, list_schemas, list_tables, query, resources, search, stats

mcp = FastMCP("database_mcp", lifespan=app_lifespan)

# Register all tools and resources
list_schemas.register(mcp)
list_tables.register(mcp)
describe_table.register(mcp)
query.register(mcp)
execute.register(mcp)
search.register(mcp)
stats.register(mcp)
resources.register(mcp)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Database Query Assistant MCP Server (PostgreSQL)")
    parser.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default="stdio",
        help="Transport: stdio (local) or http (remote)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port for HTTP transport (default: 8000)",
    )
    args = parser.parse_args()

    if args.transport == "http":
        mcp.run(transport="streamable_http", port=args.port)
    else:
        mcp.run()
