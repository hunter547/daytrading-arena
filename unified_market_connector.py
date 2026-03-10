"""
Agent trigger service — periodically publishes a prompt to ``agent_router.input``
so that trading agents wake up and decide whether to act.

Agents pull market data on-demand via the ``topstepx_retrieve_bars`` tool and
check their portfolio via ``topstepx_portfolio``.  This service only provides
the periodic heartbeat.

Usage:
    python unified_market_connector.py \
        --bootstrap-servers localhost:9092 \
        --trigger-interval 60
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import uuid_utils
from calfkit._vendor.pydantic_ai import ModelRequest
from calfkit.broker.broker import BrokerClient
from calfkit.models.event_envelope import EventEnvelope
from calfkit.nodes.agent_router_node import AgentRouterNode
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

load_dotenv()


def _is_market_open() -> bool:
    """Check if CME futures market is open.

    Sunday 18:00 ET – Friday 17:00 ET, daily maintenance 17:00–18:00 ET.
    """
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(ZoneInfo("America/New_York"))
    is_dst = now_et.dst().total_seconds() > 0

    weekday = now_utc.weekday()  # 0=Mon, 6=Sun
    t = now_utc.hour * 60 + now_utc.minute  # minutes since midnight UTC

    if is_dst:
        open_time = 22 * 60        # 22:00 UTC (18:00 EDT)
        close_time = 20 * 60 + 10  # 20:10 UTC (16:10 EDT)
    else:
        open_time = 23 * 60        # 23:00 UTC (18:00 EST)
        close_time = 21 * 60 + 10  # 21:10 UTC (16:10 EST)

    if weekday == 5:  # Saturday — always closed
        return False
    if weekday == 6:  # Sunday — only open after open_time
        return t >= open_time
    if weekday == 4:  # Friday — only open until close_time
        return t < close_time

    # Mon-Thu: closed during daily maintenance
    if close_time <= t < open_time:
        return False

    return True


async def _publish_trigger(broker: BrokerClient, router_node: AgentRouterNode) -> None:
    """Send a lightweight trigger prompt to agent_router.input."""
    now_et = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
    prompt = (
        f"Market check — {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}.\n"
        "Use your tools to check portfolio, retrieve bars, and decide whether to trade."
    )

    correlation_id = uuid_utils.uuid7().hex
    envelope = EventEnvelope(
        trace_id=correlation_id,
        patch_model_request_params=None,
        thread_id=None,
        system_message=router_node.system_message,
        final_response_topic=None,
        deps={"invoked_at": time.time()},
    )
    envelope.mark_as_start_of_turn()
    envelope.prepare_uncommitted_agent_messages(
        [ModelRequest.user_text_prompt(prompt)]
    )
    if router_node.name is not None:
        envelope.name = router_node.name

    if not broker._connection:
        await broker.start()

    await broker.publish(
        envelope,
        topic=router_node.subscribed_topic or "",
        correlation_id=correlation_id,
    )
    logger.info(f"Published trigger @ {now_et.strftime('%H:%M:%S %Z')}")


async def _trigger_loop(
    broker: BrokerClient,
    router_node: AgentRouterNode,
    interval: int,
) -> None:
    """Periodically trigger agents while the market is open."""
    logger.info(f"Trigger loop started (interval: {interval}s)")

    while True:
        try:
            if not _is_market_open():
                await asyncio.sleep(60)
                continue

            await _publish_trigger(broker, router_node)
            await asyncio.sleep(interval)

        except Exception as e:
            logger.error(f"Error in trigger loop: {e}", exc_info=True)
            await asyncio.sleep(10)


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Agent trigger service")
    parser.add_argument(
        "--trigger-interval",
        type=int,
        default=60,
        help="Seconds between agent triggers (default: 60)",
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
        help="Kafka bootstrap servers",
    )
    args = parser.parse_args()

    kafka_servers = args.bootstrap_servers or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    broker = BrokerClient(
        bootstrap_servers=kafka_servers.split(","),
        client_id="agent-trigger",
    )

    router_node = AgentRouterNode(
        chat_node=None,
        tool_nodes=[],
        name=args.router_name,
        system_prompt="",
    )

    shutdown_event = asyncio.Event()

    def handle_shutdown(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    trigger_task = asyncio.create_task(
        _trigger_loop(broker, router_node, args.trigger_interval)
    )

    print("=" * 50)
    print("Agent Trigger Service")
    print(f"  Interval: {args.trigger_interval}s")
    print(f"  Kafka:    {kafka_servers}")
    print("=" * 50)

    await shutdown_event.wait()
    trigger_task.cancel()
    logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
