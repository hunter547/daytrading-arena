"""
Test TopstepX real-time connection and print all incoming data.
This script connects to TopstepX and prints every message received.
"""

import asyncio
import json
import logging
import os
import sys

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Reduce noise from other libraries
logging.getLogger("httpx").setLevel(logging.WARNING)


async def test_topstepx_connection():
    """Test TopstepX real-time connection."""
    from topstepx_adapter import TopstepXAdapter
    
    # Get credentials
    jwt_token = os.getenv("TOPSTEPX_JWT_TOKEN")
    username = os.getenv("TOPSTEPX_USERNAME")
    api_key = os.getenv("TOPSTEPX_API_KEY")
    
    # Try to authenticate if no token
    if not jwt_token and username and api_key:
        logger.info("No JWT token, authenticating with API key...")
        from topstepx_auth import authenticate_topstepx
        
        jwt_token = await authenticate_topstepx(username, api_key)
        if not jwt_token:
            logger.error("Authentication failed!")
            return
        logger.info("✓ Authentication successful!")
    
    if not jwt_token:
        logger.error("No JWT token available. Set TOPSTEPX_JWT_TOKEN or provide username/API key.")
        return
    
    # Track data counts
    quote_count = 0
    trade_count = 0
    depth_count = 0
    
    # Define callbacks that print everything
    def on_quote(quote):
        nonlocal quote_count
        quote_count += 1
        
        print("\n" + "=" * 80)
        print(f"📊 QUOTE #{quote_count}")
        print("=" * 80)
        print(f"Symbol:        {quote.symbol}")
        print(f"Timestamp:     {quote.timestamp}")
        print(f"Last Price:    ${quote.last_price:,.2f}")
        print(f"Bid:           ${quote.best_bid:,.2f}")
        print(f"Ask:           ${quote.best_ask:,.2f}")
        print(f"Spread:        ${quote.spread():.6f}")
        print(f"Mid:           ${quote.mid_price():.2f}")
        if quote.volume_24h:
            print(f"Volume:        {quote.volume_24h:,.0f}")
        if quote.open_24h:
            print(f"Open:          ${quote.open_24h:,.2f}")
        if quote.high_24h:
            print(f"High:          ${quote.high_24h:,.2f}")
        if quote.low_24h:
            print(f"Low:           ${quote.low_24h:,.2f}")
        print("=" * 80)
    
    def on_trade(trade):
        nonlocal trade_count
        trade_count += 1
        
        print("\n" + "=" * 80)
        print(f"💹 TRADE #{trade_count}")
        print("=" * 80)
        print(f"Symbol:        {trade.symbol}")
        print(f"Timestamp:     {trade.timestamp}")
        print(f"Side:          {trade.side.upper()}")
        print(f"Price:         ${trade.price:,.2f}")
        print(f"Size:          {trade.size}")
        if trade.trade_id:
            print(f"Trade ID:      {trade.trade_id}")
        print("=" * 80)
    
    def on_depth(depth):
        nonlocal depth_count
        depth_count += 1
        
        # Print every 10th depth update to avoid spam
        if depth_count % 10 == 0:
            print(f"\n📖 DEPTH UPDATE #{depth_count}: {depth.symbol} {depth.side.upper()} "
                  f"${depth.price:,.2f} x {depth.size}")
    
    # Create adapter
    symbols = ["CON.F.US.ES.H26", "CON.F.US.NQ.H26"]
    
    print("\n" + "=" * 80)
    print("🚀 STARTING TOPSTEPX REAL-TIME CONNECTION TEST")
    print("=" * 80)
    print(f"Symbols: {', '.join(symbols)}")
    print(f"API URL: https://api.topstepx.com")
    print(f"RTC URL: https://rtc.topstepx.com")
    print("=" * 80)
    print("\nConnecting to TopstepX SignalR...")
    print("(This will print all incoming market data)")
    print("\nPress Ctrl+C to stop")
    print("=" * 80)
    
    adapter = TopstepXAdapter(
        jwt_token=jwt_token,
        symbols=symbols,
        on_quote=on_quote,
        on_trade=on_trade,
        on_depth=on_depth,
    )
    
    try:
        # Start adapter
        await adapter.start()
        
        # Run for 60 seconds or until interrupted
        print("\n✓ Connected! Listening for data...\n")
        
        await asyncio.sleep(60)
        
        # Stop adapter
        await adapter.stop()
        
        # Print summary
        print("\n" + "=" * 80)
        print("📈 SUMMARY")
        print("=" * 80)
        print(f"Quotes received:  {quote_count}")
        print(f"Trades received:  {trade_count}")
        print(f"Depth received:   {depth_count}")
        print("=" * 80)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        await adapter.stop()
        
        print("\n" + "=" * 80)
        print("📈 SUMMARY (Partial)")
        print("=" * 80)
        print(f"Quotes received:  {quote_count}")
        print(f"Trades received:  {trade_count}")
        print(f"Depth received:   {depth_count}")
        print("=" * 80)
    
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        await adapter.stop()


if __name__ == "__main__":
    asyncio.run(test_topstepx_connection())
