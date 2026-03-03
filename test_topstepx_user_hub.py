"""
Test TopstepX User Hub SignalR connection.

This script connects to the TopstepX User Hub and prints all incoming messages
for accounts, orders, positions, and trades.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Reduce noise from other libraries
logging.getLogger("httpx").setLevel(logging.WARNING)


async def test_user_hub():
    """Test User Hub connection and print all incoming messages."""
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
    
    try:
        from signalrcore.hub_connection_builder import HubConnectionBuilder
    except ImportError:
        print("❌ signalrcore not installed. Run: pip install signalrcore")
        return
    
    # User Hub URL
    hub_url = f"https://rtc.topstepx.com/hubs/user?access_token={jwt_token}"
    
    print("=" * 80)
    print("🔍 TopstepX User Hub Connection Test")
    print("=" * 80)
    print(f"Hub URL: https://rtc.topstepx.com/hubs/user")
    print("=" * 80)
    print()
    
    # Track message counts
    message_counts = {}
    
    # Message handlers
    def on_account(args):
        """Handle GatewayUserAccount events."""
        message_counts['GatewayUserAccount'] = message_counts.get('GatewayUserAccount', 0) + 1
        count = message_counts['GatewayUserAccount']
        
        print(f"\n📊 ACCOUNT UPDATE #{count}")
        print("-" * 80)
        print(f"Raw args: {args}")
        
        if isinstance(args, list) and len(args) > 0:
            data = args[0]
            print(f"Account ID:    {data.get('id')}")
            print(f"Name:          {data.get('name')}")
            print(f"Balance:       ${data.get('balance', 0):,.2f}")
            print(f"Can Trade:     {data.get('canTrade')}")
            print(f"Is Visible:    {data.get('isVisible')}")
            print(f"Simulated:     {data.get('simulated')}")
        print("-" * 80)
    
    def on_position(args):
        """Handle GatewayUserPosition events."""
        message_counts['GatewayUserPosition'] = message_counts.get('GatewayUserPosition', 0) + 1
        count = message_counts['GatewayUserPosition']
        
        print(f"\n📍 POSITION UPDATE #{count}")
        print("-" * 80)
        print(f"Raw args: {args}")
        
        if isinstance(args, list) and len(args) > 0:
            data = args[0]
            print(f"Position ID:   {data.get('id')}")
            print(f"Account ID:    {data.get('accountId')}")
            print(f"Contract ID:   {data.get('contractId')}")
            print(f"Type:          {data.get('type')} (1=Long, 2=Short)")
            print(f"Size:          {data.get('size')}")
            print(f"Avg Price:     ${data.get('averagePrice', 0):,.2f}")
            print(f"Created:       {data.get('creationTimestamp')}")
        print("-" * 80)
    
    def on_order(args):
        """Handle GatewayUserOrder events."""
        message_counts['GatewayUserOrder'] = message_counts.get('GatewayUserOrder', 0) + 1
        count = message_counts['GatewayUserOrder']
        
        print(f"\n📋 ORDER UPDATE #{count}")
        print("-" * 80)
        print(f"Raw args: {args}")
        
        if isinstance(args, list) and len(args) > 0:
            data = args[0]
            print(f"Order ID:      {data.get('id')}")
            print(f"Account ID:    {data.get('accountId')}")
            print(f"Contract ID:   {data.get('contractId')}")
            print(f"Symbol ID:     {data.get('symbolId')}")
            print(f"Status:        {data.get('status')} (0=None, 1=Open, 2=Filled, 3=Cancelled, etc.)")
            print(f"Type:          {data.get('type')} (1=Limit, 2=Market, etc.)")
            print(f"Side:          {data.get('side')} (0=Bid, 1=Ask)")
            print(f"Size:          {data.get('size')}")
            print(f"Limit Price:   ${data.get('limitPrice', 0):,.2f}")
            print(f"Stop Price:    ${data.get('stopPrice', 0):,.2f}" if data.get('stopPrice') else "")
            print(f"Fill Volume:   {data.get('fillVolume')}")
            print(f"Filled Price:  ${data.get('filledPrice', 0):,.2f}" if data.get('filledPrice') else "")
            print(f"Custom Tag:    {data.get('customTag')}")
            print(f"Created:       {data.get('creationTimestamp')}")
            print(f"Updated:       {data.get('updateTimestamp')}")
        print("-" * 80)
    
    def on_trade(args):
        """Handle GatewayUserTrade events."""
        message_counts['GatewayUserTrade'] = message_counts.get('GatewayUserTrade', 0) + 1
        count = message_counts['GatewayUserTrade']
        
        print(f"\n💹 TRADE UPDATE #{count}")
        print("-" * 80)
        print(f"Raw args: {args}")
        
        if isinstance(args, list) and len(args) > 0:
            data = args[0]
            print(f"Trade ID:      {data.get('id')}")
            print(f"Account ID:    {data.get('accountId')}")
            print(f"Contract ID:   {data.get('contractId')}")
            print(f"Order ID:      {data.get('orderId')}")
            print(f"Side:          {data.get('side')} (0=Bid, 1=Ask)")
            print(f"Size:          {data.get('size')}")
            print(f"Price:         ${data.get('price', 0):,.2f}")
            print(f"P&L:           ${data.get('profitAndLoss', 0):,.2f}")
            print(f"Fees:          ${data.get('fees', 0):,.2f}")
            print(f"Voided:        {data.get('voided')}")
            print(f"Created:       {data.get('creationTimestamp')}")
        print("-" * 80)
    
    # Build SignalR connection
    print("Building SignalR connection...")
    connection = (
        HubConnectionBuilder()
        .with_url(
            hub_url,
            options={
                "skip_negotiation": True,
                "access_token_factory": lambda: jwt_token,
                "headers": {"Authorization": f"Bearer {jwt_token}"},
            }
        )
        .with_automatic_reconnect({
            "type": "interval",
            "intervals": [1, 2, 5, 10, 30]
        })
        .build()
    )
    
    # Register event handlers for User Hub events
    print("Registering handlers for User Hub events...")
    connection.on("GatewayUserAccount", on_account)
    connection.on("GatewayUserPosition", on_position)
    connection.on("GatewayUserOrder", on_order)
    connection.on("GatewayUserTrade", on_trade)
    
    # Connection event handlers
    connection.on_open(lambda: print("\n✅ SignalR User Hub connection OPENED\n"))
    connection.on_close(lambda: print("\n⚠️  SignalR User Hub connection CLOSED\n"))
    connection.on_error(lambda data: print(f"\n❌ SignalR ERROR: {data}\n"))
    
    try:
        print("\nStarting SignalR connection...")
        connection.start()
        
        print("✅ Connection started!")
        
        print("\n" + "=" * 80)
        print("📡 Subscribing to User Hub streams...")
        print("=" * 80)
        
        # Subscribe to all user data streams
        # Note: We're subscribing without an account ID first to see what we get
        print("  - SubscribeAccounts")
        connection.invoke("SubscribeAccounts", [])
        
        # Try subscribing to other streams too (they might send data without account ID)
        # Or the server might send us the account IDs
        print("✓ Account subscription sent!")
        print("\nNote: Order/Position/Trade subscriptions may require account ID")
        print("      which we'll get from account messages if any arrive.")
        
        print("\n" + "=" * 80)
        print("Listening for User Hub messages... (waiting 30 seconds)")
        print("=" * 80)
        print()
        
        # Wait and see if any messages arrive
        for i in range(30):
            await asyncio.sleep(1)
            if i % 5 == 4:
                total_messages = sum(message_counts.values())
                print(f"⏱️  {i+1}s elapsed | Messages received: {total_messages}")
        
        print("\n" + "=" * 80)
        print("📊 RESULTS")
        print("=" * 80)
        
        if message_counts:
            print("✅ Messages received:")
            for msg_type, count in message_counts.items():
                print(f"   {msg_type}: {count}")
        else:
            print("❌ NO MESSAGES RECEIVED")
            print("\nPossible reasons:")
            print("  1. No active positions/orders on the account")
            print("  2. Account is not trading during this session")
            print("  3. Subscription method names might be different")
            print("  4. Account might not have User Hub data permissions")
            print("\nNext steps:")
            print("  1. Try placing a test order through TopstepX platform")
            print("  2. Check if account has active positions")
            print("  3. Verify account permissions for User Hub data")
            print("  4. Contact TopstepX support for User Hub usage")
        
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        try:
            connection.stop()
            print("\n✅ Connection stopped")
        except:
            pass


if __name__ == "__main__":
    print("\n🔥 Starting User Hub test...\n")
    
    try:
        asyncio.run(test_user_hub())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)
