"""
List available TopstepX contracts.

Uses the TopstepX API to search for currently active futures contracts.
"""

import asyncio
import logging
import os
import sys

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
logger = logging.getLogger(__name__)


async def list_contracts():
    """List available contracts from TopstepX."""
    from topstepx_auth import authenticate_topstepx
    
    # Get credentials
    jwt_token = os.getenv("TOPSTEPX_JWT_TOKEN")
    username = os.getenv("TOPSTEPX_USERNAME")
    api_key = os.getenv("TOPSTEPX_API_KEY")
    
    # Authenticate if needed
    if not jwt_token:
        if not username or not api_key:
            print("Error: Need TOPSTEPX_JWT_TOKEN or TOPSTEPX_USERNAME + TOPSTEPX_API_KEY")
            return
        
        print("Authenticating...")
        jwt_token = await authenticate_topstepx(username, api_key)
        if not jwt_token:
            print("❌ Authentication failed!")
            return
        print("✅ Authenticated\n")
    
    # Search for contracts
    print("=" * 100)
    print("🔍 SEARCHING FOR AVAILABLE TOPSTEPX CONTRACTS")
    print("=" * 100)
    print()
    
    api_url = "https://api.topstepx.com/api/Market/contracts/search"
    
    # Common futures symbols
    search_terms = ["ES", "NQ", "RTY", "YM", "CL", "GC", "6E", "ZB"]
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        for symbol in search_terms:
            print(f"Searching for: {symbol}")
            print("-" * 100)
            
            try:
                response = await client.post(
                    api_url,
                    headers={"Authorization": f"Bearer {jwt_token}"},
                    json={"search": symbol, "limit": 10}
                )
                response.raise_for_status()
                
                data = response.json()
                
                if data.get("success"):
                    contracts = data.get("contracts", [])
                    
                    if contracts:
                        print(f"✅ Found {len(contracts)} contract(s):\n")
                        
                        for contract in contracts:
                            contract_id = contract.get("id", "N/A")
                            name = contract.get("name", "N/A")
                            symbol = contract.get("symbol", "N/A")
                            expiration = contract.get("expiration", "N/A")
                            
                            print(f"  Contract ID:  {contract_id}")
                            print(f"  Name:         {name}")
                            print(f"  Symbol:       {symbol}")
                            print(f"  Expiration:   {expiration}")
                            print()
                    else:
                        print(f"  No contracts found for '{symbol}'")
                        print()
                else:
                    error = data.get("errorMessage", "Unknown error")
                    print(f"  ❌ Error: {error}")
                    print()
                
            except httpx.HTTPStatusError as e:
                print(f"  ❌ HTTP Error: {e.response.status_code}")
                print(f"  Response: {e.response.text[:200]}")
                print()
            except Exception as e:
                print(f"  ❌ Error: {e}")
                print()
    
    print("=" * 100)
    print("\n💡 TIP: Use the Contract ID with the tick viewer:")
    print("   ./run.sh python topstepx_tick_viewer.py")
    print("   (Update TOPSTEPX_SYMBOLS in .env with the Contract IDs above)")
    print()


async def list_available_contracts_simple():
    """Try the simpler 'available contracts' endpoint."""
    from topstepx_auth import authenticate_topstepx
    
    jwt_token = os.getenv("TOPSTEPX_JWT_TOKEN")
    username = os.getenv("TOPSTEPX_USERNAME")
    api_key = os.getenv("TOPSTEPX_API_KEY")
    
    if not jwt_token:
        if not username or not api_key:
            return
        jwt_token = await authenticate_topstepx(username, api_key)
        if not jwt_token:
            return
    
    print("\n" + "=" * 100)
    print("📋 LISTING ALL AVAILABLE CONTRACTS")
    print("=" * 100)
    print()
    
    api_url = "https://api.topstepx.com/api/Market/contracts/available"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(
                api_url,
                headers={"Authorization": f"Bearer {jwt_token}"}
            )
            response.raise_for_status()
            
            data = response.json()
            
            if data.get("success"):
                contracts = data.get("contracts", [])
                
                print(f"✅ Found {len(contracts)} available contract(s):\n")
                
                # Group by symbol
                by_symbol = {}
                for contract in contracts:
                    symbol = contract.get("symbol", {}).get("id", "Unknown")
                    if symbol not in by_symbol:
                        by_symbol[symbol] = []
                    by_symbol[symbol].append(contract)
                
                for symbol, contracts_list in sorted(by_symbol.items()):
                    print(f"\n📊 {symbol}")
                    print("   " + "-" * 90)
                    
                    for contract in contracts_list[:3]:  # Show first 3
                        contract_id = contract.get("id", "N/A")
                        name = contract.get("name", "N/A")
                        expiration = contract.get("expiration", "N/A")
                        
                        print(f"   {contract_id:30s} | {name:30s} | Expires: {expiration}")
                
                print("\n" + "=" * 100)
            else:
                error = data.get("errorMessage", "Unknown error")
                print(f"❌ Error: {error}")
                
        except Exception as e:
            print(f"❌ Error: {e}")
            print("\nTrying search method instead...")


if __name__ == "__main__":
    try:
        asyncio.run(list_available_contracts_simple())
        asyncio.run(list_contracts())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
