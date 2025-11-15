"""Query service for SQL validation and execution."""

import asyncio
import logging
import re
import time
from typing import Optional
import sqlparse
from app import db
from app.config import settings
from app.models import QueryColumn, QueryResult

logger = logging.getLogger(__name__)


class ReadOnlyQueryError(Exception):
    """Raised when a query violates read-only constraints."""
    pass


class QueryTimeoutError(Exception):
    """Raised when a query exceeds the timeout limit."""
    pass


class QueryService:
    """Service for validating and executing SQL queries."""
    
    # Dangerous keywords that indicate write operations
    FORBIDDEN_KEYWORDS = {
        'insert', 'update', 'delete', 'merge',
        'create', 'alter', 'drop', 'truncate',
        'attach', 'detach', 'copy', 'pragma',
        'call', 'execute'
    }
    
    @staticmethod
    def validate_read_only(sql: str) -> None:
        """Validate that SQL query is read-only.
        
        Args:
            sql: SQL query string.
            
        Raises:
            ReadOnlyQueryError: If query is not read-only.
        """
        # Normalize SQL
        normalized = sql.strip()
        if not normalized:
            raise ReadOnlyQueryError("Empty query")
        
        # Parse SQL to handle comments and strings properly
        try:
            parsed = sqlparse.parse(normalized)
            if not parsed:
                raise ReadOnlyQueryError("Unable to parse SQL")
            
            # Check for multiple statements
            if len(parsed) > 1:
                raise ReadOnlyQueryError("Multiple SQL statements not allowed")
            
            statement = parsed[0]
            
            # Get tokens, filtering out comments and whitespace
            tokens = [
                token for token in statement.flatten()
                if not token.is_whitespace and token.ttype not in (
                    sqlparse.tokens.Comment.Single,
                    sqlparse.tokens.Comment.Multiline
                )
            ]
            
            if not tokens:
                raise ReadOnlyQueryError("No valid SQL tokens found")
            
            # First meaningful token should be SELECT or WITH
            first_keyword = tokens[0].value.lower()
            if first_keyword not in ('select', 'with'):
                raise ReadOnlyQueryError(
                    f"Query must start with SELECT or WITH, got: {first_keyword.upper()}"
                )
            
            # Check for forbidden keywords in non-string tokens
            for token in tokens:
                if token.ttype not in (
                    sqlparse.tokens.String.Single,
                    sqlparse.tokens.String.Symbol
                ):
                    token_value = token.value.lower()
                    if token_value in QueryService.FORBIDDEN_KEYWORDS:
                        raise ReadOnlyQueryError(
                            f"Query contains forbidden keyword: {token_value.upper()}"
                        )
        
        except sqlparse.exceptions.SQLParseError as e:
            raise ReadOnlyQueryError(f"SQL parse error: {e}")
    
    @staticmethod
    def has_aggregation(sql: str) -> bool:
        """Check if query contains aggregation functions or GROUP BY.
        
        Args:
            sql: SQL query string.
            
        Returns:
            True if query has aggregation, False otherwise.
        """
        sql_lower = sql.lower()
        
        # Check for GROUP BY
        if re.search(r'\bgroup\s+by\b', sql_lower):
            return True
        
        # Check for common aggregation functions
        agg_functions = ['count', 'sum', 'avg', 'min', 'max', 'stddev', 'variance']
        for func in agg_functions:
            if re.search(rf'\b{func}\s*\(', sql_lower):
                return True
        
        return False
    
    @staticmethod
    async def execute_query(
        sql: str,
        params: Optional[dict] = None,
        max_rows: Optional[int] = None
    ) -> QueryResult:
        """Execute a SQL query with validation, limits, and timeout.
        
        Args:
            sql: SQL query string.
            params: Optional query parameters.
            max_rows: Optional maximum rows to return (overrides default).
            
        Returns:
            QueryResult with columns, rows, and execution metadata.
            
        Raises:
            ReadOnlyQueryError: If query is not read-only.
            QueryTimeoutError: If query exceeds timeout.
        """
        # Validate read-only
        QueryService.validate_read_only(sql)
        
        # Determine row limit
        limit = max_rows if max_rows is not None else settings.max_result_rows
        
        # Apply limit only if query doesn't have aggregation
        final_sql = sql
        if not QueryService.has_aggregation(sql):
            logger.debug(f"Applying row limit: {limit}")
        else:
            logger.debug("Query has aggregation, not applying automatic limit")
            limit = None  # Don't apply limit for aggregation queries
        
        # Execute with timeout
        start_time = time.perf_counter()
        
        try:
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            columns, rows = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    db.execute_query,
                    final_sql,
                    params,
                    limit
                ),
                timeout=settings.max_query_timeout_seconds
            )
        except asyncio.TimeoutError:
            raise QueryTimeoutError(
                f"Query exceeded timeout of {settings.max_query_timeout_seconds} seconds"
            )
        
        end_time = time.perf_counter()
        execution_ms = (end_time - start_time) * 1000
        
        # Get column types from DuckDB result
        conn = db.get_connection()
        # Re-execute to get description (column types)
        if params:
            result = conn.execute(final_sql if limit is None else f"SELECT * FROM ({sql}) AS t LIMIT {limit}", params)
        else:
            result = conn.execute(final_sql if limit is None else f"SELECT * FROM ({sql}) AS t LIMIT {limit}")
        
        column_metadata = [
            QueryColumn(name=desc[0], type=str(desc[1]))
            for desc in result.description
        ]
        
        # Convert rows to list of dicts
        rows_as_dicts = [
            {columns[i]: value for i, value in enumerate(row)}
            for row in rows
        ]
        
        logger.info(
            f"Query executed successfully: {len(rows)} rows, {execution_ms:.2f}ms"
        )
        
        return QueryResult(
            columns=column_metadata,
            rows=rows_as_dicts,
            row_count=len(rows),
            execution_ms=round(execution_ms, 2)
        )
