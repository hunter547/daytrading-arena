"""
Debug TopstepX SignalR connection.

This script tests the SignalR connection and prints all incoming messages
to help diagnose connection issues.
"""

import asyncio
import logging
import os
import sys
import time

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def debug_signalr():
    """Debug SignalR connection with verbose output."""
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
    
    # Use contracts from .env or default to Micro E-mini contracts
    symbols_env = os.getenv("TOPSTEPX_SYMBOLS", "CON.F.US.MES.H26")
    symbols = [s.strip() for s in symbols_env.split(",")]
    hub_url = f"https://rtc.topstepx.com/hubs/market?access_token={jwt_token}"
    
    print("=" * 80)
    print("🔍 TopstepX SignalR Debug Tool")
    print("=" * 80)
    print(f"Hub URL: https://rtc.topstepx.com/hubs/market")
    print(f"Symbols: {', '.join(symbols)}")
    print("=" * 80)
    print()
    
    # Track all messages
    message_counts = {}
    
    def on_message(args, message_type):
        """Log all incoming messages."""
        message_counts[message_type] = message_counts.get(message_type, 0) + 1
        count = message_counts[message_type]
        
        print(f"\n📨 Message #{count}: {message_type}")
        print(f"   Args Type: {type(args)}")
        print(f"   Args: {args}")
        if isinstance(args, list):
            print(f"   Args Length: {len(args)}")
            for i, arg in enumerate(args):
                print(f"   Args[{i}]: {arg}")
        print("-" * 80)
    
    # Build connection
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
            "intervals": [1, 2, 5]
        })
        .build()
    )
    
    # Register handlers for all possible event names
    event_names = [
        "GatewayQuote",
        "GatewayTrade", 
        "GatewayDepth",
        "Quote",
        "Trade",
        "Depth",
        "MarketData",
        "Tick",
        "Update",
    ]
    
    print(f"Registering handlers for: {', '.join(event_names)}")
    
    for event_name in event_names:
        connection.on(event_name, lambda args, name=event_name: on_message(args, name))
    
    # Connection event handlers
    connection.on_open(lambda: print("\n✅ SignalR connection OPENED\n"))
    connection.on_close(lambda: print("\n⚠️  SignalR connection CLOSED\n"))
    connection.on_error(lambda data: print(f"\n❌ SignalR ERROR: {data}\n"))
    
    try:
        print("\nStarting SignalR connection...")
        connection.start()
        
        print("✅ Connection started!")
        print("\nSubscribing to contracts...")
        
        # Subscribe to each symbol
        for symbol in symbols:
            print(f"  Subscribing to: {symbol}")
            
            # Use .invoke() per official ProjectX docs
            try:
                connection.invoke("SubscribeContractQuotes", [symbol])
                print(f"    ✓ SubscribeContractQuotes invoked")
            except Exception as e:
                print(f"    ✗ SubscribeContractQuotes failed: {e}")
            
            try:
                connection.invoke("SubscribeContractTrades", [symbol])
                print(f"    ✓ SubscribeContractTrades invoked")
            except Exception as e:
                print(f"    ✗ SubscribeContractTrades failed: {e}")
            
            try:
                connection.invoke("SubscribeContractMarketDepth", [symbol])
                print(f"    ✓ SubscribeContractMarketDepth invoked")
            except Exception as e:
                print(f"    ✗ SubscribeContractMarketDepth failed: {e}")
        
        print("\n" + "=" * 80)
        print("Listening for messages... (waiting 30 seconds)")
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
            print("  1. Market is closed (check CME trading hours)")
            print("  2. Subscription method incorrect")
            print("  3. Contract symbols invalid or expired")
            print("  4. Account doesn't have market data permissions")
            print("  5. Wrong hub URL or message format")
            print("\nNext steps:")
            print("  1. Check if market is open (CME hours: Sun 5pm - Fri 4pm CT)")
            print("  2. Verify contract symbols are current (H25 = March 2025)")
            print("  3. Check TopstepX account for data permissions")
            print("  4. Contact TopstepX support for correct SignalR usage")
        
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
    print("\n🔥 Starting SignalR debug tool...\n")
    
    try:
        asyncio.run(debug_signalr())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)
