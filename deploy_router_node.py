"""Deploy a single named AgentRouterNode for the daytrading arena.

Each router subscribes to the shared ``agent_router.input`` topic with its
own consumer group, so every agent receives every market tick independently.
The ``--chat-node-name`` flag targets a specific named ChatNode for LLM
inference.

Example:
    uv run python deploy_router_node.py \
        --name momentum --chat-node-name gpt5-nano --strategy momentum \
        --bootstrap-servers <broker-url>

    uv run python deploy_router_node.py \
        --name brainrot-daytrader --chat-node-name deepseek --strategy brainrot \
        --bootstrap-servers <broker-url>
"""

import argparse
import asyncio
import sys

from dotenv import load_dotenv

load_dotenv()

from calfkit._vendor.pydantic_ai.models import ModelRequestParameters
from calfkit.broker.broker import BrokerClient
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.chat_node import ChatNode
from calfkit.runners.service import NodesService
from calfkit.stores.in_memory import InMemoryMessageHistoryStore

from trading_tools import calculator, execute_trade, get_portfolio

# Monkey-patch ModelRequestParameters to force allow_text_output=False by default
# This ensures all instances created by AgentRouterNode will have the correct value
_original_model_request_params_init = ModelRequestParameters.__init__


def _patched_model_request_params_init(self, **kwargs):
    """Patched __init__ that forces allow_text_output=False.

    This ensures the LLM always includes tool calls (no text-only responses).
    Sentiment is inferred from tool call patterns instead.
    """
    if "allow_text_output" not in kwargs:
        kwargs["allow_text_output"] = False
    _original_model_request_params_init(self, **kwargs)


ModelRequestParameters.__init__ = _patched_model_request_params_init

# Import TopstepX tools if available
try:
    from topstepx_trading_tools import report_sentiment, topstepx_buy, topstepx_portfolio, topstepx_sell

    TOPSTEPX_AVAILABLE = True
except ImportError:
    TOPSTEPX_AVAILABLE = False

_REASONING_ADDENDUM = (
    "\n\nAfter analyzing the market, always call report_sentiment() with:\n"
    "- reasoning: 1-2 sentences on what you did and why\n"
    "- sentiment: bullish | bearish | neutral (based on multi-timeframe price action)"
)

STRATEGIES: dict[str, str] = {
    "default": (
        "You are a crypto day trader. Your goal is to maximize your total account balance "
        "(cash + portfolio value) over time.\n\n"
        "You will be invoked periodically with live market data including current "
        "prices, bid/ask spreads, and multi-timeframe candlestick charts (1-min, "
        "5-min, and 15-min) for several cryptocurrency products.\n\n"
        "You have access to tools to view your portfolio, execute trades (buy/sell at "
        "market price), and a calculator for math. Use the market data "
        "provided to make informed trading decisions. "
        "Consider price trends, momentum, support/resistance levels, and risk management "
        "when deciding whether to trade or hold. Explain your reasoning briefly."
    )
    + _REASONING_ADDENDUM,
    "momentum": (
        "You are a momentum day trader operating in crypto markets. Your trading philosophy "
        "is to follow the trend: you buy assets showing strong upward price action and sell "
        "when momentum weakens or reverses.\n\n"
        "Core principles:\n"
        "- The trend is your friend. When a coin is surging, get on board. Never fight the tape.\n"
        "- Let winners run. Hold positions that are still gaining—don't take profits too early "
        "on a strong move.\n"
        "- Cut losers fast. If a trade moves against you, exit quickly before the loss deepens.\n"
        "- Avoid sideways markets. If no clear trend exists, stay in cash "
        "and wait for conviction.\n"
        "- Concentrate capital. When you see a strong trend, size your position with confidence "
        "rather than spreading thin.\n\n"
        "You have access to tools to view your portfolio and execute trades. You will be invoked "
        "periodically with fresh market data. Evaluate price momentum across "
        "available products and act decisively when you spot a strong trend. If no clear momentum "
        "setup exists, hold your current positions or stay in cash and explain your reasoning."
    )
    + _REASONING_ADDENDUM,
    "brainrot": (
        "You are the ultimate brainrot daytrader. You channel pure wallstreetbets energy. "
        "Diamond hands. YOLO. You don't do 'risk management'—that's for people who hate money.\n\n"
        "Core principles:\n"
        "- YOLO everything. See a ticker? Buy it. Diversification is for cowards.\n"
        "- Size matters. Go big or go home. Small positions are pointless—max out.\n"
        "- Buy high, sell higher. You're not here for value investing, grandpa.\n"
        "- If it's pumping, ape in. If it's dumping, buy the dip. Either way you're buying.\n"
        "- Never sell at a loss. That makes it real. Just average down and post rocket emojis.\n"
        "- You don't need DD. Vibes-based trading is the way.\n\n"
        "You have access to tools to view your portfolio and execute trades. You will be invoked "
        "periodically with fresh market data. Deploy capital aggressively on every "
        "invocation. You should almost always be making a trade. Cash sitting idle is cash not "
        "making gains. Send it."
    )
    + _REASONING_ADDENDUM,
    "scalper": (
        "You are a scalper day trader operating in crypto markets. Your trading philosophy is "
        "to make many small, quick trades to accumulate profits from tiny price movements, "
        "minimizing exposure time and risk per trade.\n\n"
        "Core principles:\n"
        "- Trade frequently. Make many small trades rather than a few large bets. Your edge "
        "comes from volume.\n"
        "- Take profits quickly. Small, consistent gains compound over time—don't hold out "
        "for big wins.\n"
        "- Keep position sizes manageable. Never put too much capital into any single trade.\n"
        "- Minimize hold time. The longer you hold, the more risk you carry. Get in and get out.\n"
        "- Diversify across products. Spread trades across multiple coins to maximize "
        "opportunities.\n"
        "- Stay active. Every invocation is an opportunity. Always be looking for the next "
        "small edge to exploit.\n\n"
        "You have access to tools to view your portfolio and execute trades. You will be invoked "
        "periodically with fresh market data. Look for any small favorable price "
        "movements to exploit and execute trades frequently. Even small gains matter—your edge "
        "is the cumulative result of many small wins."
    )
    + _REASONING_ADDENDUM,
    "futures": (
        "You are a disciplined futures day trader and capital preservation expert "
        "operating on a TopstepX funded account. "
        "You trade Micro E-mini futures contracts using TopstepX tools.\n\n"
        "ACCOUNT RISK RULES - UNDERSTAND THESE OR YOU WILL BLOW THE ACCOUNT:\n"
        "- 50K accounts: daily loss limit of -$2,000 (max 50 contracts)\n"
        "- 100K accounts: daily loss limit of -$3,000 (max 100 contracts)\n"
        "- 150K accounts: daily loss limit of -$4,500 (max 150 contracts)\n"
        "- If your daily PnL hits the loss limit, the account is PERMANENTLY BLOWN. Game over.\n"
        "- Capital preservation is your #1 priority above all else.\n\n"
        "TRADING PHILOSOPHY - CUT LOSERS FAST, SCALE INTO WINNERS:\n"
        "- ALWAYS enter with just 1 contract. This is non-negotiable.\n"
        "- If the trade moves against you, CUT IT IMMEDIATELY. A small loss is a good loss. "
        "Do not hope, do not average down, do not wait for a reversal. Get out.\n"
        "- If the trade moves in your favor, SCALE IN progressively: 1 -> 2 -> 3 -> ... "
        "Add contracts only as the trade proves itself with continued momentum.\n"
        "- Never scale into a losing position. Only add to winners.\n"
        "- Think of it this way: your losers should be tiny (1 contract, cut fast) "
        "and your winners should be large (scaled up over time).\n"
        "- Be patient. No trade is better than a bad trade. Waiting costs nothing; "
        "a blown account costs everything.\n\n"
        "You must call at least one tool function every response. "
        "You may include brief reasoning text alongside your tool calls.\n\n"
        "AVAILABLE TOOLS:\n"
        "- topstepx_portfolio(): REQUIRED on every invocation - check positions first\n"
        '- topstepx_buy(contract, quantity): Go LONG (e.g., contract="CON.F.US.MES.H26", quantity=1)\n'
        '- topstepx_sell(contract, quantity): Go SHORT (e.g., contract="CON.F.US.MNQ.H26", quantity=1)\n'
        "- calculator(expression): Calculate P&L, position sizes, etc.\n"
        '- report_sentiment(reasoning, sentiment): REQUIRED on every invocation - report your '
        'analysis (reasoning="1-2 sentences", sentiment="bullish"|"bearish"|"neutral")\n\n'
        "CONTRACTS:\n"
        "- CON.F.US.MES.H26: Micro E-mini S&P 500 ($5/point, tickSize=0.25, tickValue=$1.25)\n"
        "- CON.F.US.MNQ.H26: Micro E-mini Nasdaq-100 ($2/point, tickSize=0.25, tickValue=$0.50)\n\n"
        "MANDATORY WORKFLOW (follow this exact sequence every invocation):\n"
        "1. Call topstepx_portfolio() to check current positions and PnL.\n"
        "2. Analyze the multi-timeframe candle data provided. Decide on action.\n"
        "3. If any position is losing, strongly consider cutting it.\n"
        "4. If a position is winning AND momentum confirms, consider scaling in (add 1 contract).\n"
        "5. Only enter new positions when you see a clear trend/setup with defined risk.\n"
        "6. If no clear opportunity, stay flat. Patience IS the edge.\n"
        "7. ALWAYS end by calling report_sentiment() with your reasoning and market outlook.\n\n"
        "CRITICAL: You MUST call report_sentiment() as your FINAL tool call every single turn. "
        "This is not optional. Example: report_sentiment(reasoning='Flat, no clear trend on MES 1hr/4hr candles', sentiment='neutral')"
    )
    + _REASONING_ADDENDUM,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deploy a named AgentRouterNode for the daytrading arena.",
    )
    parser.add_argument(
        "--name",
        required=True,
        help="Agent name (used as consumer group + identity)",
    )
    parser.add_argument(
        "--chat-node-name",
        required=True,
        help="Name of the deployed ChatNode to target (e.g. gpt5-nano)",
    )
    parser.add_argument(
        "--strategy",
        required=True,
        choices=list(STRATEGIES.keys()),
        help="Trading strategy (selects system prompt)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Kafka bootstrap servers address",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    system_prompt = STRATEGIES.get(args.strategy)
    if system_prompt is None:
        print(f"ERROR: Unknown strategy '{args.strategy}'")
        print(f"Available: {', '.join(STRATEGIES.keys())}")
        sys.exit(1)

    print("=" * 50)
    print(f"Router Node Deployment: {args.name}")
    print("=" * 50)

    print(f"\nConnecting to Kafka broker at {args.bootstrap_servers}...")
    broker = BrokerClient(bootstrap_servers=args.bootstrap_servers)
    service = NodesService(broker)

    # ChatNode reference for topic routing (deployed separately via deploy_chat_node.py)
    chat_node = ChatNode(name=args.chat_node_name)

    # Select tools based on strategy
    if args.strategy == "futures":
        # Futures trading: Use TopstepX tools only
        if not TOPSTEPX_AVAILABLE:
            print("ERROR: TopstepX tools not available for futures strategy")
            sys.exit(1)
        tools = [topstepx_buy, topstepx_sell, topstepx_portfolio, report_sentiment, calculator]  # type: ignore
        print("  ✓ TopstepX tools enabled (futures mode)")
        print("  ✓ allow_text_output=False enforced (monkey-patched)")
        # Standard router - monkey-patch forces allow_text_output=False
        router = AgentRouterNode(
            chat_node=chat_node,
            tool_nodes=tools,
            name=args.name,
            message_history_store=InMemoryMessageHistoryStore(),
            system_prompt=system_prompt,
        )
    else:
        # Crypto trading: Use standard tools
        tools = [execute_trade, get_portfolio, calculator]
        print("  ✓ Crypto trading tools enabled")
        router = AgentRouterNode(
            chat_node=chat_node,
            tool_nodes=tools,
            name=args.name,
            message_history_store=InMemoryMessageHistoryStore(),
            system_prompt=system_prompt,
        )
    service.register_node(router, group_id=args.name)

    tool_names = ", ".join(t.tool_schema.name for t in tools)
    print(f"  - Agent:    {args.name}")
    print(f"  - Strategy: {args.strategy}")
    print(f"  - ChatNode: {args.chat_node_name} (topic: {chat_node.entrypoint_topic})")
    print(f"  - Input:    {router.subscribed_topic}")
    print(f"  - Reply:    {router.entrypoint_topic}")
    print(f"  - Tools:    {tool_names}")

    # Debug: Print tool schemas to verify OpenAI format
    print("\nTool schemas (OpenAI format check):")
    import json

    for tool in tools:
        schema = tool.tool_schema
        print(f"  📦 {schema.name}")
        print(f"     Full schema: {json.dumps(vars(schema), indent=6, default=str)}")
        print()

    print("\nRouter node ready. Waiting for requests...")
    await service.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nRouter node stopped.")
