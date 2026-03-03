"""
Test TopstepX authentication with different username formats.
"""

import asyncio
import os
import sys

from topstepx_auth import TopstepXAuth


async def test_auth_variants():
    """Test authentication with different username formats."""
    
    api_key = os.getenv("TOPSTEPX_API_KEY")
    if not api_key:
        print("Error: TOPSTEPX_API_KEY environment variable required")
        sys.exit(1)
    
    # Try different username formats
    email = os.getenv("TOPSTEPX_USERNAME", "")
    
    if '@' in email:
        username_variants = [
            email,  # Full email: hunter547@gmail.com
            email.split('@')[0],  # Just username: hunter547
        ]
    else:
        username_variants = [email]
    
    print("=" * 70)
    print("Testing TopstepX Authentication")
    print("=" * 70)
    print(f"API Key: {api_key[:10]}...{api_key[-10:]}")
    print(f"Environment: topstepx-direct")
    print(f"API URL: https://api.topstepx.com")
    print()
    
    print("Testing username variants:")
    print("-" * 70)
    
    auth = TopstepXAuth(environment="topstepx-direct", api_base_url="https://api.topstepx.com")
    
    for i, username in enumerate(username_variants, 1):
        print(f"\n{i}. Trying username: '{username}'")
        print("   " + "-" * 60)
        
        token = await auth.login_with_api_key(username, api_key)
        
        if token:
            print(f"   ✓ SUCCESS! Token obtained.")
            print(f"   Token: {token[:50]}...")
            print()
            print("=" * 70)
            print("WORKING CONFIGURATION:")
            print("=" * 70)
            print(f"TOPSTEPX_USERNAME={username}")
            print(f"TOPSTEPX_API_KEY={api_key}")
            print("TOPSTEPX_ENVIRONMENT=topstepx-direct")
            print("TOPSTEPX_API_URL=https://api.topstepx.com")
            print("=" * 70)
            await auth.close()
            return 0
        else:
            print(f"   ✗ Failed")
    
    await auth.close()
    
    print()
    print("=" * 70)
    print("All authentication attempts failed!")
    print("=" * 70)
    print()
    print("Possible issues:")
    print("  1. API key is incorrect or expired")
    print("  2. API access not enabled on your TopstepX account")
    print("  3. Account username is different from email")
    print()
    print("Next steps:")
    print("  1. Log into TopstepX dashboard")
    print("  2. Go to API settings")
    print("  3. Verify API key is correct")
    print("  4. Check if there's a separate 'username' field (not email)")
    print("  5. Ensure API access is enabled")
    print()
    
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(test_auth_variants()))
