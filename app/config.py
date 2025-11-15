"""Configuration management for the DuckDB Query API."""

from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Application settings
    app_name: str = "DuckDB Query API"
    app_version: str = "1.0.0"
    debug: bool = False
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    
    # DuckDB settings
    duckdb_path: Optional[str] = None
    duckdb_read_only: bool = True
    duckdb_threads: int = 4
    duckdb_memory_limit: str = "1GB"
    
    # Azure extension performance tuning
    azure_read_transfer_concurrency: int = 5
    azure_read_transfer_chunk_size: int = 1048576  # 1MB
    azure_read_buffer_size: int = 1048576  # 1MB
    azure_http_stats: bool = False
    
    # Query execution settings
    max_query_timeout_seconds: int = 30
    max_result_rows: int = 10000
    
    # Logging
    log_level: str = "INFO"
    
    # Microsoft Fabric settings
    fabric_auth_method: str = "browser"  # 'auto', 'cli', 'browser', or 'default'
    fabric_workspace_name: Optional[str] = None
    fabric_lakehouse_name: Optional[str] = None
    fabric_tables: str = ""  # Comma-separated list
    
    # Azure Service Principal (for production)
    azure_tenant_id: Optional[str] = None
    azure_client_id: Optional[str] = None
    azure_client_secret: Optional[str] = None
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    @property
    def tables_list(self) -> list[str]:
        """Parse FABRIC_TABLES into a list of table names."""
        if not self.fabric_tables:
            return []
        return [t.strip() for t in self.fabric_tables.split(",") if t.strip()]
    
    @property
    def has_service_principal(self) -> bool:
        """Check if service principal credentials are fully configured."""
        return all([
            self.azure_tenant_id,
            self.azure_client_id,
            self.azure_client_secret
        ])


# Singleton instance
settings = Settings()
