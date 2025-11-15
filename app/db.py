"""DuckDB connection and table registration management.

This module handles DuckDB initialization and registration of Fabric Lakehouse tables.
"""

import logging
from typing import Optional
import duckdb
from app.config import Settings
from app.fabric_client import FabricClient

logger = logging.getLogger(__name__)

# Global DuckDB connection
_connection: Optional[duckdb.DuckDBPyConnection] = None


def initialize_duckdb(settings: Settings, fabric_client: FabricClient) -> duckdb.DuckDBPyConnection:
    """Initialize DuckDB connection with settings and register Fabric tables.
    
    Args:
        settings: Application settings.
        fabric_client: Fabric client for authentication and table paths.
        
    Returns:
        DuckDB connection object.
    """
    global _connection
    
    # Determine connection path
    db_path = settings.duckdb_path if settings.duckdb_path else ":memory:"
    
    # In-memory databases cannot be read-only
    is_read_only = settings.duckdb_read_only and db_path != ":memory:"
    
    logger.info(f"Initializing DuckDB connection: {db_path} (read_only={is_read_only})")
    
    # Create connection
    _connection = duckdb.connect(
        database=db_path,
        read_only=is_read_only
    )
    
    # Apply runtime settings
    logger.info(f"Configuring DuckDB: threads={settings.duckdb_threads}, memory_limit={settings.duckdb_memory_limit}")
    _connection.execute(f"SET threads = {settings.duckdb_threads};")
    _connection.execute(f"SET memory_limit = '{settings.duckdb_memory_limit}';")
    
    # Apply performance tuning for remote queries
    logger.info("Applying performance tuning for remote Azure queries")
    _connection.execute(f"SET preserve_insertion_order = false;")  # Reduce memory for large transfers
    
    logger.info("Installing and loading Azure extension")
    _connection.execute("INSTALL azure;")
    _connection.execute("LOAD azure;")
    
    # Configure Azure extension for optimal remote query performance
    logger.info("Configuring Azure extension performance settings")
    logger.info(f"  - transfer_concurrency={settings.azure_read_transfer_concurrency}")
    logger.info(f"  - transfer_chunk_size={settings.azure_read_transfer_chunk_size} bytes")
    logger.info(f"  - read_buffer_size={settings.azure_read_buffer_size} bytes")
    
    _connection.execute(f"SET azure_read_transfer_concurrency = {settings.azure_read_transfer_concurrency};")
    _connection.execute(f"SET azure_read_transfer_chunk_size = {settings.azure_read_transfer_chunk_size};")
    _connection.execute(f"SET azure_read_buffer_size = {settings.azure_read_buffer_size};")
    _connection.execute(f"SET azure_http_stats = {str(settings.azure_http_stats).lower()};")
    
    logger.info("✓ Azure extension performance settings applied")
    
    # Configure Azure transport to use curl (fixes SSL cert issues in Docker)
    logger.info("Configuring Azure transport to use curl for SSL/CA handling")
    try:
        _connection.execute("SET azure_transport_option_type = 'curl';")
        logger.info("✓ Azure transport set to curl")
    except Exception as e:
        logger.warning(f"Could not set azure_transport_option_type: {e}")
    
    logger.info("Configuring Azure authentication in DuckDB")
    access_token = fabric_client.get_access_token()
    
    # Note: Tokens expire, so you may need to refresh for long-running sessions
    _connection.execute(f"""
        CREATE SECRET azure_secret (
            TYPE AZURE,
            PROVIDER ACCESS_TOKEN,
            ACCOUNT_NAME 'onelake',
            ACCESS_TOKEN '{access_token}'
        );
    """)
    logger.info("✓ Azure secret configured")
    
    logger.info("Installing and loading Delta extension")
    _connection.execute("INSTALL delta;")
    _connection.execute("LOAD delta;")
    logger.info("✓ Delta extension loaded")
    
    logger.info("Registering Fabric tables as views")
    register_fabric_tables(_connection, fabric_client, settings.tables_list)
    
    logger.info("DuckDB initialization complete")
    return _connection


def register_fabric_tables(
    conn: duckdb.DuckDBPyConnection,
    fabric_client: FabricClient,
    tables: list[str]
) -> None:
    """Register Fabric Lakehouse tables as DuckDB views using delta_scan.
    
    This follows working_example.ipynb pattern - creating views that wrap delta_scan()
    so tables can be queried normally: SELECT * FROM customers
    
    Args:
        conn: DuckDB connection.
        fabric_client: Fabric client for table paths.
        tables: List of table names to register.
    """
    if not tables:
        logger.warning("No tables specified in FABRIC_TABLES configuration")
        return
    
    logger.info(f"Registering {len(tables)} Fabric tables as views")
    
    for table_name in tables:
        try:
            # Get the ABFSS path for the table
            table_path = fabric_client.build_table_path(table_name)
            
            # Create view using delta_scan (wraps the delta_scan call)
            # This allows: SELECT * FROM customers
            # Instead of: SELECT * FROM delta_scan('abfss://...')
            conn.execute(f"""
                CREATE OR REPLACE VIEW {table_name} AS 
                SELECT * FROM delta_scan('{table_path}')
            """)
            
            logger.info(f"✓ Registered table: {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to register table '{table_name}': {e}")
            # Continue with other tables rather than failing completely
            continue


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get the global DuckDB connection.
    
    Returns:
        DuckDB connection object.
        
    Raises:
        RuntimeError: If connection has not been initialized.
    """
    if _connection is None:
        raise RuntimeError("DuckDB connection not initialized. Call initialize_duckdb first.")
    return _connection


def execute_query(
    sql: str,
    params: Optional[dict] = None,
    limit: Optional[int] = None
) -> tuple[list[str], list[tuple]]:
    """Execute a SQL query against DuckDB.
    
    Args:
        sql: SQL query string.
        params: Optional query parameters.
        limit: Optional row limit to enforce.
        
    Returns:
        Tuple of (column_names, rows).
    """
    conn = get_connection()
    
    # Apply limit if specified and query doesn't have one
    final_sql = sql
    if limit is not None:
        final_sql = f"SELECT * FROM ({sql}) AS t LIMIT {limit}"
    
    # Execute query
    if params:
        result = conn.execute(final_sql, params)
    else:
        result = conn.execute(final_sql)
    
    # Fetch results
    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()
    
    return columns, rows


def close_connection() -> None:
    """Close the DuckDB connection."""
    global _connection
    if _connection is not None:
        logger.info("Closing DuckDB connection")
        _connection.close()
        _connection = None
