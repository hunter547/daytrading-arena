"""
Quick test to verify TopstepX trading tools are working.
Tests portfolio query without executing any real trades.
"""

import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

async def test_tools():
    """Test TopstepX trading tools."""
    import topstepx_trading_tools
    from calfkit.models.tool_context import ToolContext
    
    print("=" * 60)
    print("Testing TopstepX Trading Tools")
    print("=" * 60)
    
    # Initialize client
    print("\n1. Initializing client...")
    topstepx_trading_tools._init_client()
    
    if topstepx_trading_tools._trading_client is None:
        print("   ❌ Client not initialized. Check TOPSTEPX_JWT_TOKEN in .env")
        return False
    print("   ✓ Client initialized")
    
    # Get practice account
    print("\n2. Finding practice account...")
    account_id = await topstepx_trading_tools._ensure_practice_account()
    
    if account_id is None:
        print("   ❌ No practice account found")
        return False
    print(f"   ✓ Practice account found: {account_id}")
    
    # Test getting account summary
    print("\n3. Getting account summary...")
    summary = await topstepx_trading_tools._trading_client.get_account_summary(account_id)
    
    print(f"\n   Account: {summary.get('name')}")
    print(f"   Equity: ${summary.get('equity', 0):,.2f}")
    print(f"   Positions: {len(summary.get('positions', []))}")
    
    if summary.get('positions'):
        print("\n   Open Positions:")
        for pos in summary['positions']:
            print(f"     - {pos['symbol']}: {pos['quantity']} @ ${pos['avgPrice']:,.2f}")
    
    print("=" * 60)
    print("✓ All tests passed!")
    print("=" * 60)
    print("\nReady to connect agents!")
    print("\nTo deploy trading tools:")
    print("  ./run.sh python topstepx_trading_tools.py --bootstrap-servers localhost:9092")
    
    # Cleanup
    await topstepx_trading_tools._trading_client.close()
    
    return True

if __name__ == "__main__":
    success = asyncio.run(test_tools())
    exit(0 if success else 1)
