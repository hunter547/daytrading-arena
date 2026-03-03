"""
TopstepX Connection Diagnostic Tool

This script helps diagnose connection issues with TopstepX API.
"""

import asyncio
import sys

import httpx


async def test_environment(env_name: str) -> dict:
    """Test if an environment is accessible."""
    url = f"https://gateway-api-{env_name}.s2f.projectx.com"
    
    result = {
        "environment": env_name,
        "url": url,
        "dns_resolves": False,
        "http_status": None,
        "reachable": False,
    }
    
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(url)
            result["dns_resolves"] = True
            result["http_status"] = response.status_code
            result["reachable"] = True
    except httpx.ConnectError:
        # DNS resolved but connection failed
        result["dns_resolves"] = True
        result["reachable"] = False
    except Exception as e:
        # DNS or other error
        error_str = str(e)
        if "Name or service not known" in error_str or "Could not resolve" in error_str:
            result["dns_resolves"] = False
        result["error"] = error_str
    
    return result


async def test_auth_endpoint(env_name: str, username: str = "test", api_key: str = "test") -> dict:
    """Test the authentication endpoint."""
    url = f"https://gateway-api-{env_name}.s2f.projectx.com/api/Auth/loginKey"
    
    result = {
        "environment": env_name,
        "endpoint": "/api/Auth/loginKey",
        "url": url,
        "status_code": None,
        "response": None,
        "works": False,
    }
    
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(
                url,
                json={"userName": username, "apiKey": api_key},
                headers={"Content-Type": "application/json"},
            )
            result["status_code"] = response.status_code
            
            # Even with invalid creds, we should get 401 or similar, not 404
            if response.status_code != 404:
                result["works"] = True
                try:
                    result["response"] = response.json()
                except:
                    result["response"] = response.text[:100]
    except Exception as e:
        result["error"] = str(e)
    
    return result


async def main():
    """Run diagnostics."""
    print("=" * 70)
    print("TopstepX API Connection Diagnostic Tool")
    print("=" * 70)
    print()
    
    # Environments to test
    environments = [
        "demo",
        "topstepx", 
        "topstep",
        "alpha-ticks",
        "aqua-futures",
        "blusky",
        "e8x",
    ]
    
    print("Step 1: Testing Base URLs")
    print("-" * 70)
    
    reachable_envs = []
    
    for env in environments:
        print(f"Testing {env:20s} ... ", end="", flush=True)
        result = await test_environment(env)
        
        if result["dns_resolves"]:
            if result["reachable"]:
                print(f"✓ DNS OK, HTTP {result['http_status']}")
                reachable_envs.append(env)
            else:
                print(f"✓ DNS OK, ✗ Connection failed")
        else:
            print(f"✗ DNS failed")
    
    print()
    print("Step 2: Testing Authentication Endpoints")
    print("-" * 70)
    
    working_envs = []
    
    for env in reachable_envs:
        print(f"Testing {env:20s} ... ", end="", flush=True)
        result = await test_auth_endpoint(env)
        
        if result["works"]:
            print(f"✓ Endpoint exists (HTTP {result['status_code']})")
            working_envs.append(env)
        else:
            if result["status_code"] == 404:
                print(f"✗ Endpoint not found (404)")
            else:
                print(f"✗ Error: {result.get('error', 'Unknown')}")
    
    print()
    print("=" * 70)
    print("RESULTS")
    print("=" * 70)
    
    if working_envs:
        print(f"✓ Found {len(working_envs)} working environment(s):")
        for env in working_envs:
            print(f"  - {env}")
        print()
        print("RECOMMENDATION:")
        print(f"  Use: TOPSTEPX_ENVIRONMENT={working_envs[0]}")
        print()
        print("  Update your .env file:")
        print(f"    TOPSTEPX_ENVIRONMENT={working_envs[0]}")
    else:
        print("✗ No working environments found!")
        print()
        print("POSSIBLE REASONS:")
        print("  1. You need a different API endpoint URL from TopstepX support")
        print("  2. Your account may not have API access enabled")
        print("  3. TopstepX may use a custom subdomain for your firm")
        print()
        print("NEXT STEPS:")
        print("  1. Contact TopstepX support and ask for:")
        print("     - Your API endpoint URL")
        print("     - Confirmation that API access is enabled")
        print("  2. Check your TopstepX dashboard for API documentation")
        print("  3. Look for any emails from TopstepX with API setup info")
    
    print("=" * 70)
    
    return 0 if working_envs else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
