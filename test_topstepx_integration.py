"""
Quick test to verify TopstepX integration in trading_tools.
"""

import asyncio
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)

async def test_integration():
    """Test that TopstepX client initializes and fetches accounts."""
    
    # Import after load_dotenv
    from trading_tools import _topstepx_client, view
    
    if _topstepx_client is None:
        print("❌ TopstepX client not initialized")
        print("   Make sure TOPSTEPX_JWT_TOKEN is set in .env")
        return False
    
    print("✓ TopstepX client initialized")
    
    # Manually trigger account fetch
    try:
        accounts = await _topstepx_client.get_accounts()
        print(f"✓ Fetched {len(accounts)} TopstepX accounts")
        
        for i, acc in enumerate(accounts, 1):
            print(f"  {i}. {acc.name} (ID: {acc.account_id})")
            print(f"     Equity: ${acc.equity:,.2f}, Positions: {len(acc.positions)}")
        
        # Update view
        view._topstepx_accounts = accounts
        print(f"\n✓ View updated with {len(accounts)} TopstepX accounts")
        
        # Build layout to test rendering
        layout = view._build_layout()
        print("✓ Layout built successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if _topstepx_client:
            await _topstepx_client.close()

if __name__ == "__main__":
    success = asyncio.run(test_integration())
    exit(0 if success else 1)
