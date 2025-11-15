"""MCP Server for DuckDB FastAPI Integration.

This MCP server exposes DuckDB query capabilities to VS Code Copilot,
allowing direct database queries through the Model Context Protocol.

This server acts as a proxy to the FastAPI endpoint running in Docker.
"""

import asyncio
import json
import logging
import os
import sys
from typing import Any

# Ensure UTF-8 encoding for stdout/stderr on Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import httpx
from dotenv import load_dotenv
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Resource, Tool, TextContent, ImageContent, EmbeddedResource

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr  # MCP uses stdout for protocol, stderr for logs
)
logger = logging.getLogger("mcp_duckdb_server")

# Function to get table names from .env
def get_table_names_from_env() -> list[str]:
    """Load table names from FABRIC_TABLES environment variable."""
    fabric_tables = os.getenv("FABRIC_TABLES", "")
    if not fabric_tables:
        logger.warning("FABRIC_TABLES not found in .env file")
        return []
    
    # Parse comma-separated list and add  suffix
    tables = [f"{table.strip()}" for table in fabric_tables.split(",") if table.strip()]
    logger.info(f"Loaded tables from .env: {', '.join(tables)}")
    return tables

# Global state
API_BASE_URL = os.getenv("DUCKDB_API_URL", "http://localhost:8000")
available_tables: list[str] = get_table_names_from_env()
http_client: httpx.AsyncClient = None
server = Server("duckdb-fabric-server")


async def initialize_server():
    """Initialize HTTP client for FastAPI communication."""
    global http_client
    
    logger.info("Initializing MCP Server for DuckDB FastAPI")
    logger.info(f"API Base URL: {API_BASE_URL}")
    logger.info(f"Available tables from .env: {', '.join(available_tables)}")
    
    # Create async HTTP client
    http_client = httpx.AsyncClient(
        base_url=API_BASE_URL,
        timeout=60.0
    )
    
    # Test connection
    try:
        response = await http_client.get("/health")
        response.raise_for_status()
        health_data = response.json()
        logger.info(f"Connected to API: {health_data.get('status')}")
    except Exception as e:
        logger.error(f"Failed to connect to API: {e}")
        logger.error(f"Make sure the FastAPI server is running at {API_BASE_URL}")
        raise


@server.list_resources()
async def list_resources() -> list[Resource]:
    """List available database resources (tables/views)."""
    resources = []
    
    # Use table names from .env
    for table_name in available_tables:
        uri = f"duckdb://main/{table_name}"
        resources.append(Resource(
            uri=uri,
            name=table_name,
            description=f"Table: {table_name}",
            mimeType="application/json"
        ))
    
    return resources


@server.read_resource()
async def read_resource(uri: str) -> str:
    """Read resource content (table schema and sample data)."""
    if not uri.startswith("duckdb://"):
        raise ValueError(f"Invalid URI scheme: {uri}")
    
    # Parse URI: duckdb://schema/table
    parts = uri.replace("duckdb://", "").split("/")
    if len(parts) != 2:
        raise ValueError(f"Invalid URI format: {uri}")
    
    schema, table_name = parts
    
    try:
        # Get table schema via API
        schema_query = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        
        schema_response = await http_client.post(
            "/query",
            json={"sql": schema_query}
        )
        schema_response.raise_for_status()
        schema_data = schema_response.json()
        
        # Get sample data via API
        sample_query = f"SELECT * FROM {table_name} LIMIT 10"
        sample_response = await http_client.post(
            "/query",
            json={"sql": sample_query}
        )
        sample_response.raise_for_status()
        sample_data = sample_response.json()
        
        # Format response
        response = {
            "table": table_name,
            "schema": schema_data.get("rows", []),
            "sample_data": sample_data.get("rows", []),
            "row_count": len(sample_data.get("rows", []))
        }
        
        return json.dumps(response, indent=2, default=str)
    
    except Exception as e:
        logger.error(f"Error reading resource {uri}: {e}")
        raise


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools for querying DuckDB."""
    return [
        Tool(
            name="query_duckdb",
            description=(
                "PRIMARY TOOL: Execute SQL queries and ALWAYS return actual data from the database. "
                "CRITICAL: This tool MUST execute the query and return ACTUAL DATA ROWS - NEVER give users options or ask them to choose. "
                "When user asks for data,use sample data tools first then analyze and immediately write and execute the SQL query, then return the results. "
                "\n\n"
                "Use this tool for ANY question about data:\n"
                "- 'Show me sales data' - Execute: SELECT * FROM sales_transactions LIMIT 10\n"
                "- 'Find top 10 customers by revenue' - Execute the query with ORDER BY and LIMIT\n"
                "- 'Best selling products' - Execute: SELECT product_id, SUM(total_amount) as revenue FROM sales_transactions GROUP BY product_id ORDER BY revenue DESC LIMIT 10\n"
                "- 'Total sales by year' - Execute the aggregation query immediately\n"
                "\n"
                "IMPORTANT SQL SYNTAX - DuckDB follows PostgreSQL dialect:\n"
                "- Use PostgreSQL-style date functions: EXTRACT(YEAR FROM date_column)\n"
                "- String concatenation: Use || operator (e.g., first_name || ' ' || last_name)\n"
                "- CAST function: CAST(column AS DATE), CAST(column AS INTEGER)\n"
                "- Window functions: ROW_NUMBER() OVER (PARTITION BY col ORDER BY col)\n"
                "- CTEs supported: WITH cte AS (SELECT ...) SELECT * FROM cte\n"
                "- Date intervals: CURRENT_DATE - INTERVAL '30 days'\n"
                "- COALESCE, NULLIF, CASE WHEN supported\n"
                "\n"
                "Available tables:\n"
                "- customers: customer information\n"
                "- products: product catalog\n"
                "- sales_transactions: sales transactions\n"
                "- web_analytics: web analytics data\n"
                "\n"
                "ALWAYS execute queries immediately and return the actual data. "
                "Do NOT present query options to users - just run the most appropriate query."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": (
                            "Complete SQL query using DuckDB/PostgreSQL syntax. "
                            "Examples:\n"
                            "- SELECT * FROM sales_transactions WHERE status = 'Completed' LIMIT 10\n"
                            "- SELECT product_id, SUM(quantity) as total FROM sales_transactions GROUP BY product_id ORDER BY total DESC LIMIT 10\n"
                            "- SELECT EXTRACT(YEAR FROM transaction_date) as year, SUM(total_amount) as revenue FROM sales_transactions GROUP BY year ORDER BY year\n"
                            "Always use proper DuckDB/PostgreSQL syntax."
                        )
                    },
                },
                "required": ["sql"]
            }
        ),
        Tool(
            name="get_sample_data",
            description=(
                "SAMPLE DATA TOOL: Get a quick 5-row sample from any table to understand its structure and content. "
                "Use this tool to preview table data before writing complex queries. "
                "This helps understand column names, data types, and example values. "
                "\n\n"
                "Use this when:\n"
                "- User asks 'show me sample data from [table]'\n"
                "- You need to see table structure before writing a complex query\n"
                "- You want to understand what data looks like\n"
                "\n"
                "This returns 5 sample rows with all columns. "
                "After seeing the sample, use query_duckdb for actual analysis."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": (
                            "Name of the table to sample. "
                            "Available tables: customers, products, sales_transactions, web_analytics"
                        )
                    }
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="list_tables",
            description=(
                "DISCOVERY TOOL: List all available tables in the database. "
                "Use this ONLY when the user explicitly asks 'what tables are available' "
                "or 'show me the tables'. This returns table names only, not data. "
                "If the user wants data from a table, use query_duckdb instead."
            ),
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="describe_table",
            description=(
                "SCHEMA TOOL: Get column names and data types for a specific table. "
                "Use this ONLY when the user asks about table structure, columns, or schema. "
                "Examples: 'What columns are in the sales table?', 'Describe the customers table'. "
                "This returns metadata only, not actual data. "
                "If the user wants actual data, use query_duckdb instead."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to describe (e.g., 'sales_transactions')"
                    }
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="get_table_stats",
            description=(
                "STATISTICS TOOL: Get basic statistics about a table (row count). "
                "Use this ONLY when the user explicitly asks 'how many rows' or 'table size'. "
                "This returns counts only, not actual data. "
                "If the user wants actual data or analysis, use query_duckdb instead."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table (e.g., 'products')"
                    }
                },
                "required": ["table_name"]
            }
        )
    ]


@server.call_tool()
async def call_tool(name: str, arguments: Any) -> list[TextContent]:
    """Handle tool execution."""
    try:
        if name == "query_duckdb":
            return await execute_query(arguments)
        elif name == "get_sample_data":
            return await get_sample_data(arguments)
        elif name == "list_tables":
            return await list_tables_tool()
        elif name == "describe_table":
            return await describe_table_tool(arguments)
        elif name == "get_table_stats":
            return await get_table_stats_tool(arguments)
        else:
            raise ValueError(f"Unknown tool: {name}")
    except Exception as e:
        logger.error(f"Tool execution error ({name}): {e}")
        return [TextContent(
            type="text",
            text=f"Error: {str(e)}"
        )]


async def execute_query(arguments: dict) -> list[TextContent]:
    """Execute a SQL query via the FastAPI endpoint."""
    sql = arguments.get("sql", "").strip()
    limit = min(arguments.get("limit", 100), 10000)
    
    if not sql:
        raise ValueError("SQL query is required")
    
    # Add LIMIT if not present
    if "limit" not in sql.lower():
        sql = f"{sql} LIMIT {limit}"
    
    logger.info(f"Executing query via API: {sql[:100]}...")
    
    try:
        # Execute query via API
        response = await http_client.post(
            "/query",
            json={"sql": sql}
        )
        response.raise_for_status()
        result = response.json()
        
        rows = result.get("rows", [])
        columns = result.get("columns", [])
        
        # Format response with clear data presentation
        formatted_response = {
            "success": True,
            "query_executed": sql,
            "columns": columns,
            "data": rows,
            "row_count": result.get("row_count", 0),
            "execution_time_ms": result.get("execution_time_ms", 0),
            "message": f"Query returned {len(rows)} rows"
        }
        
        # Create a human-readable summary
        summary_lines = [
            f"✓ Query executed successfully",
            f"✓ Returned {len(rows)} rows in {result.get('execution_time_ms', 0):.2f}ms",
            f"\nColumns: {', '.join([col['name'] for col in columns])}",
            f"\nData (showing first {min(5, len(rows))} rows):"
        ]
        
        # Add sample data for readability
        if rows:
            import json as json_lib
            for i, row in enumerate(rows[:5]):
                summary_lines.append(f"  Row {i+1}: {json_lib.dumps(row, default=str)}")
        
        summary_lines.append(f"\nFull JSON result:")
        summary_lines.append(json.dumps(formatted_response, indent=2, default=str))
        
        return [TextContent(
            type="text",
            text="\n".join(summary_lines)
        )]
    
    except httpx.HTTPStatusError as e:
        error_detail = e.response.json().get("detail", str(e))
        raise ValueError(f"Query failed: {error_detail}")
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        raise


async def get_sample_data(arguments: dict) -> list[TextContent]:
    """Get 5 sample rows from a table to understand its structure and content."""
    table_name = arguments.get("table_name", "").strip()
    
    if not table_name:
        raise ValueError("table_name is required")
    
    # Remove schema prefix if present
    if "." in table_name:
        _, table_name = table_name.split(".", 1)
    
    logger.info(f"Getting sample data from table: {table_name}")
    
    try:
        # Query first 5 rows
        sql = f"SELECT * FROM {table_name} LIMIT 5"
        
        response = await http_client.post(
            "/query",
            json={"sql": sql}
        )
        response.raise_for_status()
        result = response.json()
        
        rows = result.get("rows", [])
        columns = result.get("columns", [])
        
        # Create detailed sample data output
        summary_lines = [
            f"✓ Sample data from table: {table_name}",
            f"✓ Retrieved {len(rows)} sample rows",
            f"\n{'='*60}",
            f"TABLE STRUCTURE:",
            f"{'='*60}",
            f"\nColumns ({len(columns)}):"
        ]
        
        # List all columns with types
        for col in columns:
            summary_lines.append(f"  - {col['name']}: {col['type']}")
        
        summary_lines.append(f"\n{'='*60}")
        summary_lines.append(f"SAMPLE DATA (5 rows):")
        summary_lines.append(f"{'='*60}\n")
        
        # Display each row in a readable format
        if rows:
            import json as json_lib
            for i, row in enumerate(rows, 1):
                summary_lines.append(f"Row {i}:")
                for col in columns:
                    col_name = col['name']
                    value = row.get(col_name)
                    summary_lines.append(f"  {col_name}: {value}")
                summary_lines.append("")  # Empty line between rows
        else:
            summary_lines.append("  (No data in table)")
        
        summary_lines.append(f"{'='*60}")
        summary_lines.append("\nUse this information to write queries with query_duckdb tool.")
        summary_lines.append(f"Example: SELECT column1, column2 FROM {table_name} WHERE condition")
        
        return [TextContent(
            type="text",
            text="\n".join(summary_lines)
        )]
    
    except httpx.HTTPStatusError as e:
        error_detail = e.response.json().get("detail", str(e))
        raise ValueError(f"Failed to get sample data: {error_detail}")
    except Exception as e:
        logger.error(f"Error getting sample data from {table_name}: {e}")
        raise


async def list_tables_tool() -> list[TextContent]:
    """List all tables and views via API."""
    # Use table names from .env
    tables = [
        {
            "name": table_name,
            "type": "VIEW",
            "full_name": table_name
        }
        for table_name in available_tables
    ]
    
    return [TextContent(
        type="text",
        text=json.dumps({"tables": tables, "count": len(tables)}, indent=2)
    )]


async def describe_table_tool(arguments: dict) -> list[TextContent]:
    """Describe table schema via API."""
    table_name = arguments.get("table_name", "").strip()
    
    if not table_name:
        raise ValueError("table_name is required")
    
    # Remove schema prefix if present
    if "." in table_name:
        _, table_name = table_name.split(".", 1)
    
    try:
        # Get column information via API
        sql = f"""
            SELECT 
                column_name, 
                data_type, 
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        
        response = await http_client.post("/query", json={"sql": sql})
        response.raise_for_status()
        result = response.json()
        
        rows = result.get("rows", [])
        if not rows:
            raise ValueError(f"Table {table_name} not found")
        
        columns = [
            {
                "name": row.get("column_name"),
                "type": row.get("data_type"),
                "nullable": row.get("is_nullable") == "YES",
                "default": row.get("column_default")
            }
            for row in rows
        ]
        
        return [TextContent(
            type="text",
            text=json.dumps({
                "table": table_name,
                "columns": columns,
                "column_count": len(columns)
            }, indent=2)
        )]
    except Exception as e:
        logger.error(f"Error describing table {table_name}: {e}")
        raise


async def get_table_stats_tool(arguments: dict) -> list[TextContent]:
    """Get table statistics via API."""
    table_name = arguments.get("table_name", "").strip()
    
    if not table_name:
        raise ValueError("table_name is required")
    
    # Remove schema prefix if present
    if "." in table_name:
        _, table_name = table_name.split(".", 1)
    
    try:
        # Get row count via API
        sql = f"SELECT COUNT(*) as count FROM {table_name}"
        
        response = await http_client.post("/query", json={"sql": sql})
        response.raise_for_status()
        result = response.json()
        
        row_count = result.get("rows", [{}])[0].get("count", 0)
        
        stats = {
            "table": table_name,
            "row_count": row_count
        }
        
        return [TextContent(
            type="text",
            text=json.dumps(stats, indent=2)
        )]
    except Exception as e:
        logger.error(f"Error getting stats for {table_name}: {e}")
        raise


async def main():
    """Run the MCP server."""
    logger.info("Starting DuckDB FastAPI MCP Server")
    
    try:
        # Initialize HTTP client
        await initialize_server()
        
        # Run server with stdio transport
        async with stdio_server() as (read_stream, write_stream):
            logger.info("MCP Server running on stdio")
            await server.run(
                read_stream,
                write_stream,
                server.create_initialization_options()
            )
    finally:
        # Cleanup
        if http_client:
            await http_client.aclose()
            logger.info("HTTP client closed")


if __name__ == "__main__":
    asyncio.run(main())
