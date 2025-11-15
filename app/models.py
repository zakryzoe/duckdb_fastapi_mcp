"""Pydantic models for API request and response structures."""

from typing import Any, Optional
from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    """Request model for query execution."""
    
    sql: str = Field(..., description="SQL query string to execute")
    params: Optional[dict[str, Any]] = Field(
        None,
        description="Optional query parameters for parameterized queries"
    )
    max_rows: Optional[int] = Field(
        None,
        description="Maximum number of rows to return (overrides default limit)"
    )
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "sql": "SELECT * FROM customers LIMIT 10",
                    "params": None,
                    "max_rows": 100
                }
            ]
        }
    }


class QueryColumn(BaseModel):
    """Column metadata in query result."""
    
    name: str = Field(..., description="Column name")
    type: str = Field(..., description="Column data type")


class QueryResult(BaseModel):
    """Successful query result."""
    
    columns: list[QueryColumn] = Field(..., description="Column metadata")
    rows: list[dict[str, Any]] = Field(..., description="Result rows as list of dictionaries")
    row_count: int = Field(..., description="Number of rows returned")
    execution_ms: float = Field(..., description="Query execution time in milliseconds")
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "columns": [
                        {"name": "customer_id", "type": "INTEGER"},
                        {"name": "first_name", "type": "VARCHAR"}
                    ],
                    "rows": [
                        {"customer_id": 1, "first_name": "John"},
                        {"customer_id": 2, "first_name": "Jane"}
                    ],
                    "row_count": 2,
                    "execution_ms": 12.34
                }
            ]
        }
    }


class QueryError(BaseModel):
    """Error response for failed queries."""
    
    detail: str = Field(..., description="Error message")
    error_type: str = Field(..., description="Type of error that occurred")
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "detail": "Query is not read-only: contains INSERT statement",
                    "error_type": "ReadOnlyViolation"
                }
            ]
        }
    }


class HealthResponse(BaseModel):
    """Health check response."""
    
    status: str = Field(..., description="Service status")
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "status": "ok"
                }
            ]
        }
    }
