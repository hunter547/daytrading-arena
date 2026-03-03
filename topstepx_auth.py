"""
TopstepX authentication helper.

This module helps you authenticate with TopstepX using your API key
and obtain a JWT token for use with the adapters.
"""

import asyncio
import logging
import os
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


class TopstepXAuth:
    """Helper class for TopstepX authentication."""
    
    def __init__(self, environment: str = "demo", api_base_url: Optional[str] = None):
        """Initialize authentication helper.
        
        Args:
            environment: Environment name (demo, topstepx, alpha-ticks, etc.)
            api_base_url: Custom API base URL (overrides environment-based URL)
        """
        self._environment = environment
        
        # Use TopstepX direct API URL (official)
        # API Endpoint: https://api.topstepx.com
        if api_base_url:
            # Custom API URL provided
            self._api_base = api_base_url.rstrip('/')
        else:
            # Use official TopstepX API
            self._api_base = "https://api.topstepx.com"
        
        self._http_client = httpx.AsyncClient(timeout=30.0)
    
    async def login_with_api_key(
        self,
        username: str,
        api_key: str,
    ) -> Optional[str]:
        """Authenticate with API key and get JWT token.
        
        Args:
            username: Your TopstepX username
            api_key: Your API key from TopstepX
            
        Returns:
            JWT token string if successful, None otherwise
        """
        url = f"{self._api_base}/api/Auth/loginKey"
        payload = {
            "userName": username,
            "apiKey": api_key,
        }
        
        try:
            logger.info(f"Authenticating with TopstepX ({self._environment})...")
            response = await self._http_client.post(url, json=payload)
            response.raise_for_status()
            
            data = response.json()
            
            if not data.get("success"):
                error_msg = data.get("errorMessage", "Unknown error")
                error_code = data.get("errorCode", -1)
                logger.error(
                    f"Authentication failed: [{error_code}] {error_msg}"
                )
                logger.debug(f"Full response: {data}")
                
                # Common error codes
                if error_code == 3:
                    logger.error(
                        "Error code 3 typically means:\n"
                        "  - Invalid username or API key\n"
                        "  - API key not enabled for your account\n"
                        "  - Username format incorrect (try with/without @domain)\n\n"
                        "Please verify:\n"
                        "  1. Username is correct (try email or just username)\n"
                        "  2. API key is copied correctly from TopstepX dashboard\n"
                        "  3. API access is enabled on your account"
                    )
                
                return None
            
            token = data.get("token")
            if token:
                logger.info("✓ Authentication successful! JWT token obtained.")
                return token
            else:
                logger.error("Authentication succeeded but no token received")
                return None
                
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error during authentication: {e.response.status_code}")
            logger.error(f"Response: {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"Error during authentication: {e}")
            return None
    
    async def validate_token(self, token: str) -> bool:
        """Validate a JWT token.
        
        Args:
            token: JWT token to validate
            
        Returns:
            True if token is valid, False otherwise
        """
        url = f"{self._api_base}/api/Auth/validateSession"
        headers = {"Authorization": f"Bearer {token}"}
        
        try:
            response = await self._http_client.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            is_valid = data.get("success", False)
            
            if is_valid:
                logger.info("✓ Token is valid")
            else:
                logger.warning("✗ Token is invalid or expired")
            
            return is_valid
            
        except Exception as e:
            logger.error(f"Error validating token: {e}")
            return False
    
    async def close(self):
        """Close HTTP client."""
        await self._http_client.aclose()


async def authenticate_topstepx(
    username: str,
    api_key: str,
    environment: str = "demo",
    api_base_url: Optional[str] = None,
) -> Optional[str]:
    """Convenience function to authenticate and get JWT token.
    
    Args:
        username: Your TopstepX username
        api_key: Your API key
        environment: Environment name (demo, topstepx, etc.)
        api_base_url: Custom API base URL (optional)
        
    Returns:
        JWT token if successful, None otherwise
        
    Example:
        >>> token = await authenticate_topstepx("myuser", "my-api-key", "demo")
        >>> if token:
        ...     print(f"Token: {token}")
    """
    auth = TopstepXAuth(environment=environment, api_base_url=api_base_url)
    try:
        token = await auth.login_with_api_key(username, api_key)
        return token
    finally:
        await auth.close()


async def main():
    """CLI tool for obtaining TopstepX JWT token."""
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(
        description="Authenticate with TopstepX and get JWT token"
    )
    parser.add_argument(
        "--username",
        type=str,
        help="TopstepX username (or set TOPSTEPX_USERNAME env var)",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        help="TopstepX API key (or set TOPSTEPX_API_KEY env var)",
    )
    parser.add_argument(
        "--environment",
        type=str,
        default="demo",
        help="Environment: demo, topstepx, alpha-ticks, etc. (default: demo)",
    )
    parser.add_argument(
        "--validate",
        type=str,
        help="Validate an existing JWT token instead of logging in",
    )
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    
    auth = TopstepXAuth(environment=args.environment)
    
    try:
        if args.validate:
            # Validate existing token
            is_valid = await auth.validate_token(args.validate)
            sys.exit(0 if is_valid else 1)
        else:
            # Get username and API key
            username = args.username or os.getenv("TOPSTEPX_USERNAME")
            api_key = args.api_key or os.getenv("TOPSTEPX_API_KEY")
            
            if not username or not api_key:
                logger.error(
                    "Username and API key required. "
                    "Provide via --username/--api-key or set "
                    "TOPSTEPX_USERNAME/TOPSTEPX_API_KEY env vars"
                )
                sys.exit(1)
            
            # Authenticate
            token = await auth.login_with_api_key(username, api_key)
            
            if token:
                print("\n" + "=" * 70)
                print("SUCCESS! Your JWT Token:")
                print("=" * 70)
                print(token)
                print("=" * 70)
                print("\nTo use this token, set it as an environment variable:")
                print(f"export TOPSTEPX_JWT_TOKEN='{token}'")
                print("\nOr add to your .env file:")
                print(f"TOPSTEPX_JWT_TOKEN={token}")
                print("\nNote: Tokens are valid for 24 hours")
                print("=" * 70)
                sys.exit(0)
            else:
                logger.error("Authentication failed")
                sys.exit(1)
    finally:
        await auth.close()


if __name__ == "__main__":
    asyncio.run(main())
