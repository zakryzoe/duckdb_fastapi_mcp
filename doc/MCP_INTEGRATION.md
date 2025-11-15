# MCP Server Integration

This project includes a Model Context Protocol (MCP) server that allows VS Code Copilot to directly query your DuckDB database via the FastAPI endpoint.

## Architecture

```
VS Code Copilot
    ↓ (JSON-RPC via stdio)
MCP Server (mcp_server.py)
    ↓ (HTTP requests)
FastAPI Server (localhost:8000)
    ↓
DuckDB Connection
    ↓
Azure Fabric Lakehouse
```

**Important:** The MCP server acts as a proxy to the FastAPI endpoint running in Docker. It does NOT connect directly to DuckDB. This ensures queries go through existing validation and security layers.

## Setup

### 1. Start the FastAPI Server

**The MCP server requires the FastAPI endpoint to be running first:**

```bash
# Using Docker (recommended)
docker-compose up -d

# Or run locally
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Verify the API is running:
```bash
curl http://localhost:8000/health
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

The MCP server requires:
- `mcp==1.1.2` - MCP protocol implementation
- `httpx` - Async HTTP client for API communication

### 3. Configure VS Code

The MCP server is automatically configured via `.vscode/mcp.json`:

```json
{
  "servers": {
    "duckdbFabric": {
      "type": "stdio",
      "command": "python",
      "args": ["${workspaceFolder}/mcp_server.py"],
      "env": {
        "PYTHONPATH": "${workspaceFolder}",
        "DUCKDB_API_URL": "http://localhost:8000"
      }
    }
  }
}
```

**Note:** The `DUCKDB_API_URL` environment variable points to your FastAPI server. Change this if your API runs on a different host/port.

### 4. Enable MCP in VS Code

1. Open VS Code Settings (Ctrl+,)
2. Search for "MCP"
3. Enable `chat.mcp.gallery.enabled`
4. Enable `chat.mcp.autostart` (optional, for auto-restart on config changes)
5. Restart VS Code

### 5. Trust the MCP Server

When you first use the MCP server:
1. VS Code will prompt you to trust the server
2. Review the configuration
3. Click "Trust" to allow the connection

## Usage

### Prerequisites

**The FastAPI server MUST be running before using the MCP server.**

Check server status:
```bash
# Check Docker container
docker ps | grep duckdb-query-api

# Check API health
curl http://localhost:8000/health
```

### Start the MCP Server

The server starts automatically when Copilot attempts to use it. You can verify it's working:

```bash
python mcp_server.py
```

### Query via Copilot

Open GitHub Copilot Chat in VS Code (Ctrl+Alt+I) and ask questions about your database:

**Example queries:**
- "What tables are available in the database?"
- "Show me the schema for the sales table"
- "Query the top 10 products by revenue"
- "How many rows are in the customers table?"
- "Show me a sample of data from the orders table"

### Available Tools

The MCP server provides these tools to Copilot:

1. **query_duckdb** - Execute read-only SQL queries (PRIMARY TOOL)
   - Automatically executes queries and returns actual data
   - Uses DuckDB/PostgreSQL SQL syntax
   - Supports SELECT, WITH (CTEs), aggregations, joins
   - Returns results with column metadata
   - Supports LIMIT parameter (max 10,000 rows)

2. **get_sample_data** - Get 5 sample rows from a table
   - Quick preview of table structure and content
   - Shows all columns with data types
   - Displays 5 example rows in readable format
   - Use before writing complex queries to understand the data
   
3. **list_tables** - List all available tables/views
   - Shows schema, name, type, and column count
   
4. **describe_table** - Get detailed schema information
   - Column names, types, nullability, defaults
   
5. **get_table_stats** - Get table statistics
   - Row count and size information

### SQL Syntax - DuckDB/PostgreSQL Compatibility

DuckDB follows PostgreSQL SQL dialect with a few differences:

**Date/Time Functions:**
```sql
-- Extract parts from dates
EXTRACT(YEAR FROM transaction_date)
EXTRACT(MONTH FROM transaction_date)

-- Date intervals
CURRENT_DATE - INTERVAL '30 days'
CURRENT_TIMESTAMP - INTERVAL '1 hour'

-- Cast to date
CAST(transaction_date AS DATE)
```

**String Operations:**
```sql
-- Concatenation
first_name || ' ' || last_name

-- Pattern matching
WHERE product_name LIKE '%Widget%'
WHERE email SIMILAR TO '%@gmail.com'
```

**Aggregations & Window Functions:**
```sql
-- Standard aggregations
SUM(total_amount), AVG(price), COUNT(*)

-- Window functions
ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC)
RANK() OVER (ORDER BY sales DESC)

-- CTEs (Common Table Expressions)
WITH monthly_sales AS (
  SELECT EXTRACT(MONTH FROM transaction_date) as month, SUM(total_amount) as revenue
  FROM sales_transactions
  GROUP BY month
)
SELECT * FROM monthly_sales ORDER BY revenue DESC
```

**Important Notes:**
- Division: `1 / 2` returns `0.5` (float division). Use `1 // 2` for integer division.
- Case insensitive identifiers (but case is preserved)
- Double equality `==` supported but not recommended (use `=`)
- Use `COALESCE()` and `NULLIF()` for NULL handling

### Available Resources

The server exposes database tables as MCP resources with URI format:
```
duckdb://{schema}/{table_name}
```

You can browse these resources in Copilot by selecting "Add Context > MCP Resources".

## Security

- **Read-only**: All queries are validated to ensure they're read-only (SELECT/WITH only)
- **Query validation**: Uses `sqlparse` to prevent SQL injection and write operations
- **Row limits**: Enforces maximum row limits to prevent excessive data transfer
- **Trust model**: VS Code requires explicit trust before starting the server

## Troubleshooting

### MCP Server Not Starting

1. **Check FastAPI is running:**
   ```bash
   curl http://localhost:8000/health
   ```
   
2. **Check the MCP output log:**
   - Command Palette > "MCP: List Servers"
   - Select "duckdbFabric" > "Show Output"
   - Look for connection errors

3. **Verify Python environment:**
   ```bash
   python -c "import mcp.server, httpx; print('OK')"
   ```

4. **Check API URL in mcp.json:**
   - Ensure `DUCKDB_API_URL` matches your FastAPI endpoint
   - Default: `http://localhost:8000`
   - If using different port: update `.vscode/mcp.json`

### No Queries Reaching Database

**Common cause:** MCP server connects to FastAPI, but FastAPI can't reach DuckDB.

1. **Check Docker container logs:**
   ```bash
   docker logs duckdb-query-api
   ```

2. **Test FastAPI endpoint directly:**
   ```bash
   curl -X POST http://localhost:8000/query \
     -H "Content-Type: application/json" \
     -d '{"sql": "SELECT 1 as test"}'
   ```

3. **Verify tables are registered:**
   ```bash
   curl http://localhost:8000/health
   # Check "tables" array in response
   ```

### Copilot Not Using MCP Tools

1. **Enable tools in Copilot chat:**
   - Open the Tools picker in Copilot chat (tool icon)
   - Ensure MCP tools from "duckdbFabric" are enabled

2. **Explicitly reference tools:**
   - Type `#query_duckdb` in your prompt

3. **Check server status:**
   - Command Palette > "MCP: List Servers"
   - Ensure "duckdbFabric" shows as running (green indicator)

### Connection Errors

**Error: "Failed to connect to API"**

1. **FastAPI not running:**
   ```bash
   docker-compose up -d
   ```

2. **Wrong API URL:**
   - Check `.vscode/mcp.json` has correct `DUCKDB_API_URL`
   - Default should be `http://localhost:8000`

3. **Port conflict:**
   - Check if port 8000 is available
   - If using different port, update both `docker-compose.yml` and `mcp.json`

**Error: "Available tables: " (empty)**

This means FastAPI is running but no tables are registered:
1. Check `.env` file has `FABRIC_TABLES` set
2. Verify Azure authentication is working
3. Check Docker container logs for initialization errors

## Development

### Debug Mode

To enable debug logging in the MCP server, modify `mcp_server.py`:

```python
logging.basicConfig(level=logging.DEBUG, ...)
```

Then restart the MCP server:
1. Command Palette > "MCP: List Servers"
2. Select "duckdbFabric" > "Restart Server"

### Test API Connectivity

### Test API Connectivity

Test that the FastAPI endpoint is accessible:

```bash
# Health check
curl http://localhost:8000/health

# Test query endpoint
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT 1 as test"}'
```

### Test MCP Server Manually

You can test the MCP server's connection to the API:

```bash
# Set API URL
export DUCKDB_API_URL="http://localhost:8000"

# Run server (it will log connection status)
python mcp_server.py
# Should see: "Connected to API: ok"
# Press Ctrl+C to exit
```

## How It Works

**Request Flow:**

1. **User asks Copilot:** "Show me sales data"
2. **Copilot decides** to use MCP tool `query_duckdb`
3. **MCP Server** receives tool call via stdio
4. **MCP Server** validates and forwards query to FastAPI:
   ```
   POST http://localhost:8000/query
   {"sql": "SELECT * FROM sales LIMIT 10"}
   ```
5. **FastAPI** validates query (read-only check)
6. **FastAPI** executes query via DuckDB → Azure Fabric
7. **Results** flow back: FastAPI → MCP Server → Copilot → User

**Key Points:**
- MCP server does NOT connect to DuckDB directly
- All queries go through FastAPI's validation layer
- Existing security and query limits are preserved
- MCP acts as a "Copilot adapter" for your API

## References

- [MCP Documentation](https://modelcontextprotocol.io/)
- [VS Code MCP Guide](https://code.visualstudio.com/docs/copilot/customization/mcp-servers)
- [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)
- [DuckDB MCP Extension](https://duckdb.org/community_extensions/extensions/duckdb_mcp)
