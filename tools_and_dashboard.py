import argparse
import asyncio
import logging

from dotenv import load_dotenv
from rich.live import Live

from calfkit.broker.broker import BrokerClient
from calfkit.runners.service import NodesService
from coinbase_kafka_connector import (
    PRICE_TOPIC,
    TickerMessage,
)
from trading_tools import (
    calculator,
    execute_trade,
    get_portfolio,
    price_book,
    view,
)

# Tools & Price Feed — Deploys trading tool workers and subscribes
# to the Kafka price topic published by the connector.
#
# The price subscriber hydrates the shared price book that the trading
# tools read from when executing trades.
#
# Usage:
#     uv run python examples/daytrading_agents_arena/tools_and_dashboard.py
#
# Prerequisites:
#     - Kafka broker running at localhost:9092

load_dotenv()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Deploy trading tools, price feed, and dashboard.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Kafka bootstrap servers address",
    )
    return parser.parse_args()


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    args = parse_args()

    print("=" * 50)
    print("Tools & Price Feed Deployment")
    print("=" * 50)

    print(f"\nConnecting to Kafka broker at {args.bootstrap_servers}...")
    broker = BrokerClient(bootstrap_servers=args.bootstrap_servers)
    service = NodesService(broker)

    # ── Tool nodes ───────────────────────────────────────────────
    print("\nRegistering trading tool nodes...")
    for tool in (execute_trade, get_portfolio, calculator):
        service.register_node(tool)
        print(f"  - {tool.tool_schema.name} (topic: {tool.subscribed_topic})")

    # ── Price subscriber ─────────────────────────────────────────
    @broker.subscriber(PRICE_TOPIC, group_id="tools-dashboard")
    async def handle_price_update(ticker: TickerMessage) -> None:
        price_book.update(ticker.model_dump())
        view.rerender()

    print("\nStarting portfolio dashboard (prices via Kafka)...")
    
    # Fetch TopstepX accounts initially if client is configured
    from trading_tools import _topstepx_client
    if _topstepx_client:
        print("Fetching TopstepX accounts...")
        await view._refresh_topstepx_accounts()
        tradeable_count = len([a for a in view._topstepx_accounts])
        print(f"  ✓ Loaded {tradeable_count} eligible TopstepX account(s)")
        if tradeable_count > 0:
            print(f"    (Filtered by: canTrade=true, above MLL, active status)")
            print(f"    (50K: ≥$48K, 100K: ≥$97K, 150K: ≥$145.5K)")
            print(f"  ✓ Accounts ready for display")

    # Build initial layout with fetched accounts
    initial_layout = view._build_layout()
    
    with Live(initial_layout, auto_refresh=False, screen=True) as live:
        view.attach_live(live)
        await service.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTools and price feed stopped.")
