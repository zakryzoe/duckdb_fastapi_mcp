"""
Azure Token Generator for Docker Deployment

This script authenticates interactively via browser and saves the access token
to a file that can be mounted into the Docker container.

Usage:
    python get_token.py

The token will be saved to .azure_token and expires after ~1 hour.
Re-run this script when the token expires.
"""

import json
import os
from datetime import datetime
from azure.identity import InteractiveBrowserCredential

TOKEN_FILE = ".azure_token"
SCOPE = "https://storage.azure.com/.default"


def get_token_interactive():
    """Get access token using interactive browser authentication."""
    print("Starting interactive browser authentication...")
    print("A browser window will open for you to sign in.")
    print()
    
    try:
        credential = InteractiveBrowserCredential()
        token = credential.get_token(SCOPE)
        
        # Save token to file
        token_data = {
            "token": token.token,
            "expires_on": token.expires_on,
            "obtained_at": datetime.now().isoformat(),
            "scope": SCOPE
        }
        
        with open(TOKEN_FILE, "w") as f:
            json.dump(token_data, f, indent=2)
        
        # Make file readable only by owner for security
        os.chmod(TOKEN_FILE, 0o600)
        
        print("Authentication successful!")
        print(f"Token saved to: {TOKEN_FILE}")
        print(f"Token expires at: {datetime.fromtimestamp(token.expires_on)}")
        print()
        print("You can now run Docker with:")
        print("  docker-compose up")
        print()
        print("Token will expire in ~1 hour. Re-run this script when needed.")
        
        return token_data
        
    except Exception as e:
        print(f"Authentication failed: {e}")
        print()
        print("Troubleshooting:")
        print("1. Ensure you have a browser available")
        print("2. Check your network connection")
        print("3. Verify you have access to the Azure subscription")
        raise


if __name__ == "__main__":
    get_token_interactive()
