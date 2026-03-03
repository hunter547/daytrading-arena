"""
TopstepX Tick Data Viewer

Connects to TopstepX and displays live streaming tick data (bid, ask, last, volume, etc.)
in real-time for CME futures contracts.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Reduce noise
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("signalrcore").setLevel(logging.WARNING)


async def stream_tick_data():
    """Stream and display live tick data from TopstepX."""
    from topstepx_adapter import TopstepXAdapter
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
        
        print("Authenticating with TopstepX...")
        jwt_token = await authenticate_topstepx(username, api_key)
        if not jwt_token:
            print("❌ Authentication failed!")
            return
        print("✅ Authentication successful!\n")
    
    # Symbols to stream
    symbols = os.getenv("TOPSTEPX_SYMBOLS", "CON.F.US.ES.H26,CON.F.US.NQ.H26").split(",")
    
    print("=" * 100)
    print("🎯 TOPSTEPX LIVE TICK DATA VIEWER")
    print("=" * 100)
    print(f"Symbols: {', '.join(symbols)}")
    print(f"API: https://api.topstepx.com")
    print(f"RTC: https://rtc.topstepx.com/hubs/market")
    print("=" * 100)
    print()
    
    # Track tick counts
    tick_counts = {symbol: 0 for symbol in symbols}
    
    # Quote callback - displays tick data
    def on_quote(quote):
        tick_counts[quote.symbol] = tick_counts.get(quote.symbol, 0) + 1
        count = tick_counts[quote.symbol]
        
        # Format timestamp
        ts = quote.timestamp.strftime("%H:%M:%S.%f")[:-3]
        
        # Calculate spread and mid
        spread = quote.best_ask - quote.best_bid
        mid = (quote.best_bid + quote.best_ask) / 2
        
        # Print tick data in a clean format
        print(f"[{ts}] {quote.symbol:20s} | "
              f"Tick #{count:5d} | "
              f"Last: {quote.last_price:8.2f} | "
              f"Bid: {quote.best_bid:8.2f} | "
              f"Ask: {quote.best_ask:8.2f} | "
              f"Spread: {spread:6.2f} | "
              f"Mid: {mid:8.2f}")
    
    # Trade callback - shows actual trades
    def on_trade(trade):
        ts = trade.timestamp.strftime("%H:%M:%S.%f")[:-3]
        side_emoji = "🟢" if trade.side == "buy" else "🔴"
        
        print(f"[{ts}] {trade.symbol:20s} | "
              f"TRADE {side_emoji} | "
              f"Price: {trade.price:8.2f} | "
              f"Size: {trade.size:6.0f} | "
              f"{trade.side.upper()}")
    
    # Depth callback - shows order book changes
    depth_count = 0
    def on_depth(depth):
        nonlocal depth_count
        depth_count += 1
        
        # Print every 50th depth update to avoid spam
        if depth_count % 50 == 0:
            ts = depth.timestamp.strftime("%H:%M:%S.%f")[:-3]
            print(f"[{ts}] {depth.symbol:20s} | "
                  f"DEPTH | "
                  f"{depth.side.upper():4s} @ {depth.price:8.2f} | "
                  f"Size: {depth.size:6.0f}")
    
    # Create adapter
    adapter = TopstepXAdapter(
        jwt_token=jwt_token,
        symbols=symbols,
        on_quote=on_quote,
        on_trade=on_trade,
        on_depth=on_depth,
    )
    
    try:
        print("Connecting to TopstepX SignalR...")
        await adapter.start()
        print("\n✅ Connected! Streaming live tick data...\n")
        print("-" * 100)
        
        # Stream indefinitely until Ctrl+C
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            print("\n\n" + "-" * 100)
            print("\n⚠️  Stopped by user\n")
    
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    
    finally:
        await adapter.stop()
        
        # Print summary
        print("\n" + "=" * 100)
        print("📊 SUMMARY")
        print("=" * 100)
        total_ticks = sum(tick_counts.values())
        print(f"Total ticks received: {total_ticks}")
        for symbol, count in tick_counts.items():
            print(f"  {symbol}: {count} ticks")
        print("=" * 100)


if __name__ == "__main__":
    print("\n🔥 Starting TopstepX Tick Data Viewer...")
    print("Press Ctrl+C to stop\n")
    
    try:
        asyncio.run(stream_tick_data())
    except KeyboardInterrupt:
        print("\nExiting...")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
