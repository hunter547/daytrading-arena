"""
Unified market data connector that works with any MarketDataAdapter.

This connector bridges market data adapters (Coinbase, TopstepX, etc.)
to the Kafka-based agent system. It receives normalized Quote/Trade/Candle
data from adapters and publishes them to the AgentRouterNode.

Usage:
    # With Coinbase
    python unified_market_connector.py --provider coinbase --symbols BTC-USD ETH-USD

    # With TopstepX
    TOPSTEPX_JWT_TOKEN=your_token python unified_market_connector.py \
        --provider topstepx --symbols CON.F.US.ES.H26 CON.F.US.NQ.H26
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
import time
from typing import Optional

import uuid_utils
from calfkit._vendor.pydantic_ai import ModelRequest
from calfkit.broker.broker import BrokerClient
from calfkit.models.event_envelope import EventEnvelope
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.runners.service_client import RouterServiceClient
from dotenv import load_dotenv

from market_data_adapter import Candle, DepthLevel, MarketDataAdapter, Quote, Trade

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

load_dotenv()

PRICE_TOPIC = "market_data.prices"
FUTURES_PRICE_TOPIC = "market_data.futures_prices"


class UnifiedMarketConnector:
    """Unified connector for any MarketDataAdapter.

    Receives normalized market data from adapters and publishes to Kafka
    for consumption by trading agents.
    """

    def __init__(
        self,
        broker: BrokerClient,
        router_node: AgentRouterNode,
        adapter: MarketDataAdapter,
        min_publish_interval: float = 0.0,
        enable_candles: bool = True,
        candle_interval_seconds: int = 60,
        candles_only: bool = False,
        account_client=None,
    ):
        """Initialize unified connector.

        Args:
            broker: Kafka broker client
            router_node: Agent router node to publish to
            adapter: Market data adapter instance
            min_publish_interval: Minimum seconds between publishes per symbol
            enable_candles: Whether to fetch and publish historical candles
            candle_interval_seconds: How often to refresh candles (default: 60s)
            candles_only: If True, disable WebSocket streaming and only fetch candles via REST
            account_client: Optional TopstepXAccountClient for portfolio injection
        """
        self._broker = broker
        self._router_node = router_node
        self._client = RouterServiceClient(broker, router_node)
        self._adapter = adapter
        self._min_interval = min_publish_interval
        self._enable_candles = enable_candles
        self._candle_interval = candle_interval_seconds
        self._candles_only = candles_only
        self._account_client = account_client

        self._running = False
        self._last_publish_time: dict[str, float] = {}
        self._latest_quotes: dict[str, Quote] = {}
        self._cached_portfolio_line: str = "CURRENT POSITIONS: NONE — you are completely flat."
        self._portfolio_cache_time: float = 0.0

        # Set adapter callbacks
        adapter._on_quote = self._on_quote
        adapter._on_trade = self._on_trade
        adapter._on_candle = self._on_candle
        adapter._on_depth = self._on_depth

    async def _get_portfolio_line(self) -> str:
        """Fetch current portfolio and return a one-line summary.

        Caches for 5 seconds to avoid excessive API calls.
        """
        now = time.time()
        if now - self._portfolio_cache_time < 5.0:
            return self._cached_portfolio_line

        if not self._account_client:
            return self._cached_portfolio_line

        try:
            accounts = await self._account_client.get_accounts()
            account_id = None
            for acct in accounts:
                if "PRAC" in acct.name:
                    account_id = int(acct.account_id)
                    break
            if not account_id:
                return self._cached_portfolio_line

            positions = await self._account_client.get_positions(account_id)
            if not positions:
                line = "CURRENT POSITIONS: NONE — you are completely flat. Do NOT call topstepx_close."
            else:
                parts = []
                for pos in positions:
                    direction = "LONG" if pos.quantity > 0 else "SHORT"
                    parts.append(
                        f"{direction} {abs(int(pos.quantity))}x {pos.symbol} "
                        f"@ ${pos.avg_price:,.2f} P&L: ${pos.unrealized_pnl:+,.2f}"
                    )
                line = f"CURRENT POSITIONS: {', '.join(parts)}"
            self._cached_portfolio_line = line
            self._portfolio_cache_time = now
        except Exception as e:
            logger.warning(f"Portfolio fetch error: {e}")

        return self._cached_portfolio_line

    async def _publish_to_agent(
        self, user_prompt: str, deps: dict | None = None
    ) -> None:
        """Publish directly to agent.

        The agent's FuturesAgentRouterNode will set allow_text_output=False
        to force OpenAI to return structured tool calls instead of text.

        Args:
            user_prompt: The prompt to send to the agent
            deps: Optional dependencies dict
        """
        correlation_id = uuid_utils.uuid7().hex

        # Create event envelope (agent will add ModelRequestParameters)
        event_envelope = EventEnvelope(
            trace_id=correlation_id,
            patch_model_request_params=None,  # Let agent set this with tools
            thread_id=None,
            system_message=self._router_node.system_message,
            final_response_topic=None,
            deps=deps,
        )
        event_envelope.mark_as_start_of_turn()
        event_envelope.prepare_uncommitted_agent_messages(
            [ModelRequest.user_text_prompt(user_prompt)]
        )
        if self._router_node.name is not None:
            event_envelope.name = self._router_node.name

        # Ensure broker is started
        if not self._broker._connection:
            await self._broker.start()

        # Publish directly to the router's subscribed topic
        await self._broker.publish(
            event_envelope,
            topic=self._router_node.subscribed_topic or "",
            correlation_id=correlation_id,
        )

    async def start(self) -> None:
        """Start the connector and begin consuming market data."""
        logger.info("Starting unified market connector")
        self._running = True

        # Start adapter WebSocket connection (unless candles-only mode)
        if not self._candles_only:
            await self._adapter.start()
            logger.info("WebSocket streaming enabled")
        else:
            logger.info("Candles-only mode: WebSocket streaming disabled")

        # Start candle refresh task if enabled
        if self._enable_candles:
            asyncio.create_task(self._refresh_candles_loop())

        logger.info("Unified market connector started")

    async def stop(self) -> None:
        """Stop the connector."""
        logger.info("Stopping unified market connector")
        self._running = False

        await self._adapter.stop()

        logger.info("Unified market connector stopped")

    def _on_quote(self, quote: Quote) -> None:
        """Handle quote update from adapter.

        Args:
            quote: Quote data
        """
        # Store latest quote
        self._latest_quotes[quote.symbol] = quote

        # Check if we should publish (throttling)
        now = time.time()
        last_publish = self._last_publish_time.get(quote.symbol, 0)

        if now - last_publish < self._min_interval:
            return  # Too soon, skip

        self._last_publish_time[quote.symbol] = now

        # Build prompt and publish
        asyncio.create_task(self._publish_quote(quote))

        # Publish price update to futures price topic for trading-tools consumption
        asyncio.create_task(self._publish_price_update(quote))

    def _on_trade(self, trade: Trade) -> None:
        """Handle trade update from adapter.

        Args:
            trade: Trade data
        """
        # Trades can be published directly without throttling
        # or you can add throttling if needed
        logger.debug(
            f"Trade: {trade.symbol} @ {trade.price} x {trade.size} ({trade.side})"
        )

    def _on_candle(self, candle: Candle) -> None:
        """Handle candle update from adapter.

        Args:
            candle: Candle data
        """
        logger.debug(
            f"Candle: {candle.symbol} {candle.timestamp} "
            f"O:{candle.open} H:{candle.high} L:{candle.low} C:{candle.close} V:{candle.volume}"
        )

    def _on_depth(self, depth: DepthLevel) -> None:
        """Handle depth update from adapter.

        Args:
            depth: Depth level data
        """
        logger.debug(
            f"Depth: {depth.symbol} {depth.side} @ {depth.price} x {depth.size}"
        )

    async def _publish_quote(self, quote: Quote) -> None:
        """Publish quote to agent router.

        Args:
            quote: Quote to publish
        """
        try:
            # Build market data prompt
            prompt_parts = [
                f"Market Update: {quote.symbol}",
                f"Time: {quote.timestamp.isoformat()}",
                f"Last Price: ${quote.last_price:,.2f}",
                f"Bid: ${quote.best_bid:,.2f} x {quote.best_bid_size}",
                f"Ask: ${quote.best_ask:,.2f} x {quote.best_ask_size}",
                f"Spread: ${quote.spread():.6f}",
            ]

            if quote.volume_24h:
                prompt_parts.append(f"24h Volume: {quote.volume_24h:,.2f}")
            if quote.open_24h:
                change = quote.last_price - quote.open_24h
                change_pct = (change / quote.open_24h) * 100
                prompt_parts.append(f"24h Change: ${change:+.2f} ({change_pct:+.2f}%)")

            prompt = "\n".join(prompt_parts)

            # Publish to agent router with allow_text_output=False
            await self._publish_to_agent(
                user_prompt=prompt,
                deps={"invoked_at": time.time()},
            )

            logger.debug(f"Published quote for {quote.symbol}")

        except Exception as e:
            logger.error(f"Error publishing quote: {e}")

    async def _publish_price_update(self, quote: Quote) -> None:
        """Publish a lightweight price update to the futures price topic.

        This is consumed by trading-tools to keep the price cache fresh.

        Args:
            quote: Quote containing latest price data
        """
        try:
            if not self._broker._connection:
                await self._broker.start()

            message = {
                "contract_id": quote.symbol,
                "price": quote.last_price,
                "timestamp": quote.timestamp.isoformat(),
            }
            await self._broker.publish(message, topic=FUTURES_PRICE_TOPIC)
            logger.debug(f"Published price update for {quote.symbol}: ${quote.last_price:,.2f}")
        except Exception as e:
            logger.error(f"Error publishing price update: {e}")

    def _has_open_positions(self) -> bool:
        """Check if portfolio cache indicates open positions."""
        line = self._cached_portfolio_line
        return bool(line) and "NONE" not in line and "CURRENT POSITIONS:" in line

    async def _refresh_candles_loop(self) -> None:
        """Periodically fetch and publish historical candles.

        When positions are open, loops immediately (no sleep) so the LLM
        can monitor and manage the trade in real time.
        """
        from datetime import timedelta

        logger.info(
            f"Starting candle refresh loop (interval: {self._candle_interval}s)"
        )

        while self._running:
            try:
                # Refresh portfolio cache before deciding sleep duration
                await self._get_portfolio_line()

                if self._has_open_positions():
                    # In a trade — no delay, loop immediately
                    pass
                else:
                    await asyncio.sleep(self._candle_interval)

                if not self._running:
                    break

                # Fetch candles for all symbols
                for symbol in self._adapter._symbols:
                    await self._fetch_and_publish_candles(symbol)

            except Exception as e:
                logger.error(f"Error in candle refresh loop: {e}")

    async def _fetch_and_publish_candles(self, symbol: str) -> None:
        """Fetch historical candles for a symbol and include in next publish.

        Args:
            symbol: Symbol to fetch candles for
        """
        from datetime import datetime, timedelta, timezone

        try:
            now = datetime.now(timezone.utc)

            # When in a trade, skip slow timeframes — only need short-term data
            if self._has_open_positions():
                timeframes = [
                    (900, 180, 90, "15-min candles (3h ago -> 90min ago)"),
                    (300, 90, 20, "5-min candles (90min ago -> 20min ago)"),
                    (60, 20, 0, "1-min candles (last 20 minutes)"),
                ]
            else:
                timeframes = [
                    (14400, 2880, 480, "4-hour candles (48h ago -> 8h ago)"),
                    (3600, 720, 120, "1-hour candles (12h ago -> 2h ago)"),
                    (900, 180, 90, "15-min candles (3h ago -> 90min ago)"),
                    (300, 90, 20, "5-min candles (90min ago -> 20min ago)"),
                    (60, 20, 0, "1-min candles (last 20 minutes)"),
                ]

            # Map contract IDs to friendly names so LLM doesn't confuse
            # market data symbols with positions it holds
            _FRIENDLY = {
                "CON.F.US.MES.H26": "E-mini S&P 500 Micro",
            }
            friendly = _FRIENDLY.get(symbol, symbol)
            prompt_parts = [
                f"\n[CHART DATA for {friendly} — this is NOT a position, just price history for analysis]"
            ]

            # Fetch all timeframes in parallel
            async def _fetch_one(granularity, lookback_mins, offset_mins, description):
                start_time = now - timedelta(minutes=lookback_mins)
                end_time = now - timedelta(minutes=offset_mins)
                try:
                    return description, await self._adapter.fetch_candles(
                        symbol=symbol,
                        granularity_seconds=granularity,
                        start_time=start_time,
                        end_time=end_time,
                        limit=100,
                    )
                except Exception as e:
                    logger.warning(f"Failed to fetch {description} for {symbol}: {e}")
                    return description, []

            results = await asyncio.gather(
                *[_fetch_one(g, lb, off, desc) for g, lb, off, desc in timeframes]
            )

            candles = []  # track last successful fetch for price publish
            for description, tf_candles in results:
                if tf_candles:
                    candles = tf_candles
                    prompt_parts.append(f"\n{description}:")
                    prompt_parts.append("Time,Open,High,Low,Close,Volume")
                    for candle in tf_candles:
                        prompt_parts.append(
                            f"{candle.timestamp.isoformat()},"
                            f"{candle.open},{candle.high},{candle.low},"
                            f"{candle.close},{candle.volume}"
                        )

            # Publish latest candle close price for live dashboard PnL
            if candles:
                latest_price = candles[-1].close
                try:
                    if not self._broker._connection:
                        await self._broker.start()
                    await self._broker.publish(
                        {"contract_id": symbol, "price": latest_price, "timestamp": now.isoformat()},
                        topic=FUTURES_PRICE_TOPIC,
                    )
                except Exception as e:
                    logger.debug(f"Error publishing candle price: {e}")

            # Get latest quote for this symbol
            quote = self._latest_quotes.get(symbol)
            if quote:
                prompt_parts.insert(
                    0,
                    f"Current Price: {quote.symbol} @ ${quote.last_price:,.2f} "
                    f"(Bid: ${quote.best_bid:,.2f}, Ask: ${quote.best_ask:,.2f})",
                )

            # Inject current portfolio state as the first line
            portfolio_line = await self._get_portfolio_line()
            prompt_parts.insert(0, portfolio_line)
            logger.info(f"📋 Portfolio injected: {portfolio_line[:80]}")

            # Publish enriched data with allow_text_output=False
            await self._publish_to_agent(
                user_prompt="\n".join(prompt_parts),
                deps={"invoked_at": time.time()},
            )

            logger.debug(f"Published candles for {symbol}")

        except Exception as e:
            logger.error(f"Error fetching/publishing candles for {symbol}: {e}", exc_info=True)


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Unified market data connector")
    parser.add_argument(
        "--provider",
        type=str,
        required=True,
        choices=["coinbase", "topstepx"],
        help="Market data provider to use",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        nargs="+",
        required=True,
        help="Symbols to subscribe to",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.0,
        help="Minimum publish interval in seconds (default: 0, no throttling)",
    )
    parser.add_argument(
        "--candle-interval",
        type=int,
        default=60,
        help="Candle refresh interval in seconds (default: 60)",
    )
    parser.add_argument(
        "--candles-only",
        action="store_true",
        default=False,
        help="Disable real-time WebSocket streaming, only fetch candles via REST API (default: False)",
    )
    parser.add_argument(
        "--router-name",
        type=str,
        default="default",
        help="Router node name (default: default)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=None,
        help="Kafka bootstrap servers (default: localhost:9092 or KAFKA_BOOTSTRAP_SERVERS env var)",
    )
    args = parser.parse_args()

    # Initialize Kafka broker
    # Priority: CLI arg > env var > default
    kafka_servers = args.bootstrap_servers or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    broker = BrokerClient(
        bootstrap_servers=kafka_servers.split(","),
        client_id="unified-market-connector",
    )

    # Create dummy router node (just for publishing)
    # In production, you'd reference the actual router node
    router_node = AgentRouterNode(
        chat_node=None,  # Not used for publishing
        tool_nodes=[],
        name=args.router_name,
        system_prompt="",
    )

    # Initialize appropriate adapter
    if args.provider == "coinbase":
        from coinbase_adapter import CoinbaseAdapter

        adapter = CoinbaseAdapter(symbols=args.symbols)
        logger.info(f"Using Coinbase adapter for: {args.symbols}")

    elif args.provider == "topstepx":
        from topstepx_adapter import TopstepXAdapter
        from topstepx_auth import authenticate_topstepx

        jwt_token = os.getenv("TOPSTEPX_JWT_TOKEN")
        environment = os.getenv("TOPSTEPX_ENVIRONMENT", "topstepx")
        api_base_url = os.getenv("TOPSTEPX_API_URL", "https://api.topstepx.com")

        # If no JWT token, try to authenticate with API key
        if not jwt_token:
            username = os.getenv("TOPSTEPX_USERNAME")
            api_key = os.getenv("TOPSTEPX_API_KEY")

            if username and api_key:
                logger.info("No JWT token found, authenticating with API key...")
                jwt_token = await authenticate_topstepx(
                    username, api_key, environment, api_base_url
                )

                if not jwt_token:
                    logger.error("Authentication with API key failed")
                    sys.exit(1)

                logger.info("✓ Authenticated successfully! Token obtained.")
            else:
                logger.error(
                    "TopstepX authentication required. Either:\n"
                    "  1. Set TOPSTEPX_JWT_TOKEN (if you have a token), OR\n"
                    "  2. Set TOPSTEPX_USERNAME and TOPSTEPX_API_KEY (to get a token)\n\n"
                    "To get a JWT token from your API key, run:\n"
                    "  python topstepx_auth.py --username YOUR_USERNAME --api-key YOUR_API_KEY"
                )
                sys.exit(1)

        adapter = TopstepXAdapter(
            jwt_token=jwt_token,
            symbols=args.symbols,
            environment=environment,
            api_base_url=api_base_url,
        )
        logger.info(f"Using TopstepX adapter ({environment}) for: {args.symbols}")

    # Create account client for portfolio injection (futures only)
    account_client = None
    if args.provider == "topstepx":
        from topstepx_account import TopstepXAccountClient
        account_client = TopstepXAccountClient(jwt_token=jwt_token)
        logger.info("✓ Account client initialized for portfolio injection")

    # Create unified connector
    connector = UnifiedMarketConnector(
        broker=broker,
        router_node=router_node,
        adapter=adapter,
        min_publish_interval=args.interval,
        candle_interval_seconds=args.candle_interval,
        candles_only=args.candles_only,
        account_client=account_client,
    )

    # Handle shutdown
    shutdown_event = asyncio.Event()

    def handle_shutdown(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Start connector
    await connector.start()

    # Wait for shutdown signal
    await shutdown_event.wait()

    # Stop connector
    await connector.stop()

    logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
