"""Fabric Lakehouse client for authentication and table path management.

"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from azure.identity import AzureCliCredential, ClientSecretCredential, InteractiveBrowserCredential, DefaultAzureCredential
from app.config import Settings

logger = logging.getLogger(__name__)

TOKEN_FILE = ".azure_token"


class FabricClient:
    """Client for authenticating to Microsoft Fabric and accessing Lakehouse tables."""
    
    def __init__(self, settings: Settings):
        """Initialize the Fabric client with settings.
        
        Args:
            settings: Application settings containing Fabric configuration.
        """
        self.settings = settings
        self.workspace_name = settings.fabric_workspace_name
        self.lakehouse_name = settings.fabric_lakehouse_name
        self._credential: Optional[Any] = None
        self._access_token: Optional[str] = None
    
    def get_credential(self) -> Any:
        """Get Azure credential for authentication.
        
        Returns credential based on priority:
        1. Service Principal (if all credentials are provided)
        2. Explicit auth method from FABRIC_AUTH_METHOD
        3. Fallback to browser-based interactive auth
        
        Returns:
            Azure credential object.
        """
        if self._credential is not None:
            return self._credential
        
        if self.settings.has_service_principal:
            logger.info("Using Service Principal authentication")
            self._credential = ClientSecretCredential(
                tenant_id=self.settings.azure_tenant_id,
                client_id=self.settings.azure_client_id,
                client_secret=self.settings.azure_client_secret
            )
            return self._credential
        
        auth_method = self.settings.fabric_auth_method.lower()
        
        if auth_method == "cli":
            logger.info("Using Azure CLI authentication")
            self._credential = AzureCliCredential()
        elif auth_method == "browser":
            logger.info("Using Interactive Browser authentication")
            self._credential = InteractiveBrowserCredential()
        elif auth_method in ("default", "auto"):
            logger.info("Using Default Azure Credential chain")
            self._credential = DefaultAzureCredential()
        else:
            logger.warning(
                f"Unknown auth method '{auth_method}', falling back to Interactive Browser"
            )
            self._credential = InteractiveBrowserCredential()
        
        return self._credential
    
    def _load_cached_token(self) -> Optional[str]:
        """Load cached token from file if available and not expired.
        
        Returns:
            Access token string if valid, None otherwise.
        """
        token_path = Path(TOKEN_FILE)
        
        if not token_path.exists():
            return None
        
        try:
            with open(token_path, "r") as f:
                token_data = json.load(f)
            
            expires_on = token_data.get("expires_on")
            if not expires_on:
                logger.warning("Token file missing expiration, ignoring cache")
                return None
            
            # Check if token is still valid (with 5 minute buffer)
            current_time = datetime.now().timestamp()
            if current_time >= (expires_on - 300):  # 5 minutes before expiry
                logger.warning("Cached token expired or expiring soon")
                return None
            
            logger.info("✓ Using cached access token from file")
            expires_at = datetime.fromtimestamp(expires_on)
            logger.info(f"  Token expires at: {expires_at}")
            
            return token_data.get("token")
            
        except Exception as e:
            logger.warning(f"Failed to load cached token: {e}")
            return None
    
    def get_access_token(self) -> str:
        """Get access token for Azure Storage.
        
        Priority:
        1. Try cached token from file (for Docker deployments)
        2. Use credential-based authentication
        
        Returns:
            Access token string.
            
        Raises:
            Exception: If authentication fails.
        """
        if self._access_token is not None:
            return self._access_token
        
        # Try cached token first 
        cached_token = self._load_cached_token()
        if cached_token:
            self._access_token = cached_token
            return self._access_token
        
        try:
            credential = self.get_credential()
            
            # Try Azure CLI first if method is CLI
            if isinstance(credential, AzureCliCredential):
                try:
                    token = credential.get_token("https://storage.azure.com/.default")
                    logger.info("✓ Authenticated using Azure CLI")
                except Exception as cli_error:
                    logger.warning(f"Azure CLI authentication failed: {cli_error}")
                    logger.info("Falling back to Interactive Browser authentication")
                    credential = InteractiveBrowserCredential()
                    token = credential.get_token("https://storage.azure.com/.default")
                    logger.info("✓ Authenticated using Interactive Browser")
            else:
                token = credential.get_token("https://storage.azure.com/.default")
                logger.info("✓ Authentication successful")
            
            self._access_token = token.token
            return self._access_token
            
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            logger.error(
                "For Docker deployments with browser auth, run 'python get_token.py' "
                "on your host machine first to generate a token file."
            )
            raise
    
    def build_table_path(self, table_name: str) -> str:
        """Build ABFSS path for a Fabric Lakehouse table.
        
        This follows the exact pattern from working_example.ipynb:
        abfss://WorkspaceName@onelake.dfs.fabric.microsoft.com/LakehouseName.Lakehouse/Tables/table_name
        
        Args:
            table_name: Name of the table.
            
        Returns:
            ABFSS path string.
            
        Raises:
            ValueError: If workspace or lakehouse name is not configured.
        """
        if not self.workspace_name or not self.lakehouse_name:
            raise ValueError(
                "FABRIC_WORKSPACE_NAME and FABRIC_LAKEHOUSE_NAME must be configured"
            )
        
        path = (
            f"abfss://{self.workspace_name}@onelake.dfs.fabric.microsoft.com/"
            f"{self.lakehouse_name}.Lakehouse/Tables/{table_name}"
        )
        
        logger.debug(f"Built table path for '{table_name}': {path}")
        return path
