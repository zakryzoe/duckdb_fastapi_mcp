# DuckDB Query API for Microsoft Fabric Lakehouse

A production-ready FastAPI service that provides a read-only SQL query interface to Microsoft Fabric Lakehouse tables using DuckDB.

## Features

- **Read-only queries** - Enforces SQL validation to prevent write operations
- **Fast execution** - Leverages DuckDB's high-performance analytics engine
- **Fabric integration** - Direct access to Microsoft Fabric Lakehouse tables via OneLake
- **Flexible authentication** - Supports Service Principal, Azure CLI, and Interactive Browser authentication
- **Delta Lake support** - Reads Delta tables directly from Fabric Lakehouse
- **Docker ready** - Production-ready containerized deployment
- **Full API docs** - Auto-generated OpenAPI/Swagger documentation

## Project Structure

```
.
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI app entrypoint
│   ├── config.py            # Configuration management
│   ├── db.py                # DuckDB connection & table registration
│   ├── models.py            # Pydantic request/response models
│   ├── fabric_client.py     # Fabric authentication & lakehouse access
│   ├── api/
│   │   ├── routes_health.py # Health check endpoint
│   │   └── routes_query.py  # Query execution endpoint
│   └── services/
│       └── query_service.py # Query validation & execution logic
├── tests/
│   ├── test_query_endpoint.py
│   └── test_validation.py
├── .env.example             # Environment configuration template
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## Quick Start

### Prerequisites

- Python 3.11+
- Microsoft Fabric workspace with Lakehouse
- Azure credentials (Service Principal, Azure CLI, or browser-based auth)

### Installation

1. Clone the repository
2. Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

3. Edit `.env` with your Fabric configuration:

```env
# Fabric configuration
FABRIC_AUTH_METHOD=browser
FABRIC_WORKSPACE_NAME=Your Workspace Name
FABRIC_LAKEHOUSE_NAME=Your Lakehouse Name
FABRIC_TABLES=customers,products,sales_transactions

# Optional: Service Principal (recommended for production)
# AZURE_TENANT_ID=your-tenant-id
# AZURE_CLIENT_ID=your-client-id
# AZURE_CLIENT_SECRET=your-client-secret
```

4. Install dependencies:

```bash
pip install -r requirements.txt
```

5. Run the application:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

The API will be available at `http://localhost:8000`

- API Documentation: `http://localhost:8000/docs`
- Health Check: `http://localhost:8000/health`

## Docker Deployment

### Build and run with Docker Compose:

```bash
docker-compose up --build
```

### Or build and run manually:

```bash
docker build -t duckdb-query-api .
docker run -p 8000:8000 --env-file .env duckdb-query-api
```

## Usage Examples

### Health Check

```bash
curl http://localhost:8000/health
```

Response:
```json
{
  "status": "ok"
}
```

### Simple Query

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM customers LIMIT 10"
  }'
```

### Aggregation Query

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT COUNT(*) as total_customers FROM customers"
  }'
```

### Query with Custom Row Limit

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM products",
    "max_rows": 100
  }'
```

### Join Multiple Tables

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT c.first_name, c.last_name, COUNT(t.transaction_id) as order_count FROM customers c LEFT JOIN sales_transactions t ON c.customer_id = t.customer_id GROUP BY c.customer_id, c.first_name, c.last_name"
  }'
```

## Authentication Methods

The application supports multiple authentication methods (configured via `FABRIC_AUTH_METHOD`):

### 1. Service Principal (Production - Recommended)
Set all three environment variables:
```env
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret
```

### 2. Interactive Browser (Default)
```env
FABRIC_AUTH_METHOD=browser
```
Opens a browser window for authentication.

**For Docker deployments**: Run `python get_token.py` on your host machine first to generate a cached token that will be mounted into the container.

### 3. Azure CLI
```env
FABRIC_AUTH_METHOD=cli
```
Uses credentials from `az login`.

### 4. Default Credential Chain
```env
FABRIC_AUTH_METHOD=default
```
Uses Azure's DefaultAzureCredential (tries multiple methods).

## Configuration

All configuration is managed through environment variables in `.env`:

| Variable | Description | Default |
|----------|-------------|---------|
| `APP_NAME` | Application name | DuckDB Query API |
| `API_PORT` | Server port | 8000 |
| `DUCKDB_THREADS` | DuckDB thread count | 4 |
| `DUCKDB_MEMORY_LIMIT` | Memory limit | 1GB |
| `MAX_QUERY_TIMEOUT_SECONDS` | Query timeout | 30 |
| `MAX_RESULT_ROWS` | Default row limit | 10000 |
| `LOG_LEVEL` | Logging level | INFO |
| `FABRIC_WORKSPACE_NAME` | Fabric workspace name | (required) |
| `FABRIC_LAKEHOUSE_NAME` | Fabric lakehouse name | (required) |
| `FABRIC_TABLES` | Comma-separated table list | (required) |

## Security Features

- **Read-only enforcement** - Blocks INSERT, UPDATE, DELETE, DROP, and other write operations
- **SQL injection protection** - Validates SQL syntax and structure
- **Query timeout** - Prevents long-running queries from blocking resources
- **Row limits** - Configurable maximum result set size
- **Non-root container** - Docker image runs as unprivileged user

## MCP Support

This project includes a Model Context Protocol (MCP) server that enables GitHub Copilot to directly query your database through natural language.

**Quick Setup:**
1. Start the FastAPI server (Docker or locally)
2. MCP server auto-starts when Copilot needs it
3. Ask Copilot questions about your data in VS Code Chat

**Example queries:**
- "Show me the top 10 customers by revenue"
- "What tables are available?"
- "Give me a sample of the sales data"

For detailed setup instructions and troubleshooting, see [MCP_INTEGRATION.md](MCP_INTEGRATION.md).

## API Endpoints

### `GET /health`
Health check endpoint.

**Response**: `200 OK`
```json
{
  "status": "ok"
}
```

### `POST /query`
Execute a read-only SQL query.

**Request Body**:
```json
{
  "sql": "SELECT * FROM table_name",
  "max_rows": 100  // Optional
}
```

**Success Response**: `200 OK`
```json
{
  "columns": [
    {"name": "id", "type": "INTEGER"},
    {"name": "name", "type": "VARCHAR"}
  ],
  "rows": [
    {"id": 1, "name": "John"},
    {"id": 2, "name": "Jane"}
  ],
  "row_count": 2,
  "execution_ms": 12.34
}
```

**Error Responses**:
- `400` - Invalid or non-read-only SQL
- `422` - Validation error
- `504` - Query timeout
- `500` - Internal server error

## Development

### Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run with auto-reload
uvicorn app.main:app --reload
```

### Code Quality

The codebase follows these principles:
- Type hints throughout
- Pydantic v2 for validation
- Comprehensive logging
- Clean separation of concerns
- Exception handling at appropriate levels

## Troubleshooting

### Authentication Issues

If you encounter authentication errors:

1. **For Service Principal**: Verify tenant ID, client ID, and client secret
2. **For Azure CLI**: Run `az login` and verify with `az account show`
3. **For Interactive Browser**: Ensure you have browser access and proper permissions

### Table Not Found

Ensure tables are:
1. Listed in `FABRIC_TABLES` environment variable
2. Exist in your Fabric Lakehouse
3. Accessible with your authentication method

### Query Timeout

If queries timeout:
1. Increase `MAX_QUERY_TIMEOUT_SECONDS`
2. Optimize your SQL query
3. Consider adding indexes in Fabric

## License

This project is provided as-is for use with Microsoft Fabric Lakehouse.

## Contributing

Contributions are welcome! Please ensure:
- Code follows existing patterns
- Tests are included for new features
- Documentation is updated

---

**MCP Session Samples**

The images in `doc/img/` are samples of the Model Context Protocol (MCP) sessions demonstrating a working connection from the MCP server to DuckDB (via Microsoft Fabric). They show the agent discovering tables, sampling rows, running count/aggregation queries, and returning tabular results.

- `doc/img/img1.png` — MCP discovery and available tables found in the lakehouse (shows table list and suggested next actions).
- `doc/img/img2.png` — Sample rows and a total-row count query executed against the `web_analytics` table (shows sample rows and the count result).
- `doc/img/img3.png` — Example analytical query: monthly top-5 purchased products for the current year (shows grouped aggregation results).

These screenshots are included as examples of how the MCP server interacts with DuckDB and formats results. To add more MCP session samples, place PNGs in `doc/img/` and reference them here, for example:

```
![MCP session example](doc/img/your-image.png)
```

---

**Built with**: FastAPI, DuckDB, and Azure SDK
