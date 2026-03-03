"""
Example usage of the market data adapter system.

This script demonstrates how to:
1. Use the Coinbase adapter
2. Use the TopstepX adapter
3. Switch between providers seamlessly
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG to see raw data
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Set specific loggers to appropriate levels
logging.getLogger("httpx").setLevel(logging.WARNING)  # Reduce httpx noise
logging.getLogger("websockets").setLevel(logging.WARNING)  # Reduce websocket noise


async def example_coinbase():
    """Example: Using the Coinbase adapter."""
    from coinbase_adapter import CoinbaseAdapter
    
    logger.info("=== Coinbase Adapter Example ===")
    
    # Define callback handlers
    def on_quote(quote):
        logger.info(
            f"Quote: {quote.symbol} @ ${quote.last_price:,.2f} | "
            f"Bid: ${quote.best_bid:,.2f} | Ask: ${quote.best_ask:,.2f} | "
            f"Spread: ${quote.spread():.6f}"
        )
    
    # Create adapter
    adapter = CoinbaseAdapter(
        symbols=["BTC-USD", "ETH-USD"],
        on_quote=on_quote,
    )
    
    # Start adapter
    await adapter.start()
    logger.info("Coinbase adapter started. Listening for quotes...")
    
    # Run for 30 seconds
    await asyncio.sleep(30)
    
    # Fetch historical candles
    logger.info("\nFetching historical candles for BTC-USD...")
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)
    
    candles = await adapter.fetch_candles(
        symbol="BTC-USD",
        granularity_seconds=300,  # 5-minute candles
        start_time=start_time,
        end_time=end_time,
        limit=10,
    )
    
    logger.info(f"Received {len(candles)} candles:")
    for candle in candles[-5:]:  # Show last 5
        logger.info(
            f"  {candle.timestamp.strftime('%H:%M:%S')} | "
            f"O:{candle.open:,.2f} H:{candle.high:,.2f} "
            f"L:{candle.low:,.2f} C:{candle.close:,.2f} "
            f"V:{candle.volume:,.2f}"
        )
    
    # Stop adapter
    await adapter.stop()
    logger.info("Coinbase adapter stopped")


async def example_topstepx():
    """Example: Using the TopstepX adapter."""
    from topstepx_adapter import TopstepXAdapter
    
    logger.info("=== TopstepX Adapter Example ===")
    
    # Get JWT token from environment
    jwt_token = os.getenv("TOPSTEPX_JWT_TOKEN")
    if not jwt_token:
        logger.error(
            "TOPSTEPX_JWT_TOKEN environment variable not set. "
            "Skipping TopstepX example."
        )
        return
    
    # Define callback handlers with detailed output
    def on_quote(quote):
        logger.info("=" * 70)
        logger.info("QUOTE DATA RECEIVED:")
        logger.info(f"  Symbol:       {quote.symbol}")
        logger.info(f"  Timestamp:    {quote.timestamp}")
        logger.info(f"  Last Price:   ${quote.last_price:,.2f}")
        logger.info(f"  Best Bid:     ${quote.best_bid:,.2f} x {quote.best_bid_size}")
        logger.info(f"  Best Ask:     ${quote.best_ask:,.2f} x {quote.best_ask_size}")
        logger.info(f"  Spread:       ${quote.spread():.6f}")
        logger.info(f"  Mid Price:    ${quote.mid_price():.2f}")
        if quote.volume_24h:
            logger.info(f"  Volume 24h:   {quote.volume_24h:,.0f}")
        if quote.open_24h:
            logger.info(f"  Open 24h:     ${quote.open_24h:,.2f}")
        if quote.high_24h:
            logger.info(f"  High 24h:     ${quote.high_24h:,.2f}")
        if quote.low_24h:
            logger.info(f"  Low 24h:      ${quote.low_24h:,.2f}")
        logger.info("=" * 70)
    
    def on_trade(trade):
        logger.info("=" * 70)
        logger.info("TRADE DATA RECEIVED:")
        logger.info(f"  Symbol:       {trade.symbol}")
        logger.info(f"  Timestamp:    {trade.timestamp}")
        logger.info(f"  Side:         {trade.side.upper()}")
        logger.info(f"  Price:        ${trade.price:,.2f}")
        logger.info(f"  Size:         {trade.size}")
        if trade.trade_id:
            logger.info(f"  Trade ID:     {trade.trade_id}")
        logger.info("=" * 70)
    
    def on_depth(depth):
        logger.info("=" * 70)
        logger.info("MARKET DEPTH DATA RECEIVED:")
        logger.info(f"  Symbol:       {depth.symbol}")
        logger.info(f"  Timestamp:    {depth.timestamp}")
        logger.info(f"  Side:         {depth.side.upper()}")
        logger.info(f"  Price:        ${depth.price:,.2f}")
        logger.info(f"  Size:         {depth.size}")
        logger.info("=" * 70)
    
    # Create adapter
    # Note: Update contract ID to a currently active futures contract
    adapter = TopstepXAdapter(
        jwt_token=jwt_token,
        symbols=["CON.F.US.ES.H26"],  # E-mini S&P March 2025
        environment="demo",
        on_quote=on_quote,
        on_trade=on_trade,
        on_depth=on_depth,
    )
    
    # Start adapter
    await adapter.start()
    logger.info("TopstepX adapter started. Listening for market data...")
    
    # Run for 30 seconds
    await asyncio.sleep(30)
    
    # Fetch historical bars
    logger.info("\nFetching historical bars for ES...")
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=2)
    
    candles = await adapter.fetch_candles(
        symbol="CON.F.US.ES.H26",
        granularity_seconds=900,  # 15-minute bars
        start_time=start_time,
        end_time=end_time,
        limit=10,
    )
    
    logger.info(f"Received {len(candles)} bars:")
    for candle in candles[-5:]:  # Show last 5
        logger.info(
            f"  {candle.timestamp.strftime('%H:%M:%S')} | "
            f"O:{candle.open:,.2f} H:{candle.high:,.2f} "
            f"L:{candle.low:,.2f} C:{candle.close:,.2f} "
            f"V:{candle.volume:,.0f}"
        )
    
    # Stop adapter
    await adapter.stop()
    logger.info("TopstepX adapter stopped")


async def example_comparison():
    """Example: Compare data from multiple providers simultaneously."""
    logger.info("=== Multi-Provider Comparison ===")
    
    # Track quotes from both providers
    quotes = {"coinbase": {}, "topstepx": {}}
    
    def coinbase_quote(quote):
        quotes["coinbase"][quote.symbol] = quote
        logger.info(f"[Coinbase] {quote.symbol}: ${quote.last_price:,.2f}")
    
    def topstepx_quote(quote):
        quotes["topstepx"][quote.symbol] = quote
        logger.info(f"[TopstepX] {quote.symbol}: ${quote.last_price:,.2f}")
    
    # Start Coinbase
    from coinbase_adapter import CoinbaseAdapter
    coinbase = CoinbaseAdapter(
        symbols=["BTC-USD"],
        on_quote=coinbase_quote,
    )
    await coinbase.start()
    
    # Start TopstepX (if token available)
    topstepx = None
    jwt_token = os.getenv("TOPSTEPX_JWT_TOKEN")
    if jwt_token:
        from topstepx_adapter import TopstepXAdapter
        topstepx = TopstepXAdapter(
            jwt_token=jwt_token,
            symbols=["CON.F.US.ES.H26"],
            environment="demo",
            on_quote=topstepx_quote,
        )
        await topstepx.start()
    
    # Run for 20 seconds
    await asyncio.sleep(20)
    
    # Stop both
    await coinbase.stop()
    if topstepx:
        await topstepx.stop()
    
    logger.info("\n=== Summary ===")
    logger.info(f"Coinbase quotes received: {len(quotes['coinbase'])}")
    logger.info(f"TopstepX quotes received: {len(quotes['topstepx'])}")


async def main():
    """Run all examples."""
    parser = argparse.ArgumentParser(description="Market data adapter examples")
    parser.add_argument(
        "--example",
        type=str,
        choices=["coinbase", "topstepx", "comparison", "all"],
        default="all",
        help="Which example to run",
    )
    
    args = parser.parse_args()
    
    try:
        if args.example in ["coinbase", "all"]:
            await example_coinbase()
            logger.info("\n" + "=" * 60 + "\n")
        
        if args.example in ["topstepx", "all"]:
            await example_topstepx()
            logger.info("\n" + "=" * 60 + "\n")
        
        if args.example in ["comparison", "all"]:
            await example_comparison()
    
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())
