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


class StatelessHistoryStore(InMemoryMessageHistoryStore):
    """History store that wipes previous history at the start of each new invocation.

    Each agent invocation starts with a UserPromptPart / TextPart in a ModelRequest.
    When we detect that, we clear all prior messages so the LLM has zero stale context.
    Within a single invocation, tool call/result round-trips are preserved normally.
    """

    async def append(self, thread_id, message, scope=None):
        from calfkit._vendor.pydantic_ai.messages import ModelRequest, UserPromptPart
        # Detect start of new invocation: ModelRequest containing user prompt
        if isinstance(message, ModelRequest):
            has_user_prompt = any(
                isinstance(p, UserPromptPart) for p in message.parts
            )
            if has_user_prompt:
                # Wipe all prior history — start fresh
                self._messages[thread_id] = []
        await super().append(thread_id, message, scope)

    async def append_many(self, thread_id, messages, scope=None):
        for msg in messages:
            await self.append(thread_id, msg, scope)

from trading_tools import calculator, execute_trade, get_portfolio

# allow_text_output=True (default) — LLM can end its turn with text.
# This prevents infinite tool-call loops when flat/throttled.
# Dashboard reasoning is ONLY updated from report_sentiment and portfolio
# tool returns (line 408 raw LLM text path is suppressed).

# Import TopstepX tools if available
try:
    from topstepx_trading_tools import (
        report_sentiment,
        topstepx_available_contracts,
        topstepx_buy,
        topstepx_close,
        topstepx_portfolio,
        topstepx_retrieve_bars,
        topstepx_sell,
    )

    TOPSTEPX_AVAILABLE = True
except ImportError:
    TOPSTEPX_AVAILABLE = False

import os

# Account tier configs keyed by account name prefix.
# max_drawdown = major loss limit (account blows if balance drops this much below high-water mark)
# daily_loss_limit = circuit breaker — stop trading for the day at this realized loss
# daily_profit_target = aim to lock in this much per day, then stop or go light
# profit_goal = overall account profit goal (the "finish line" for a funded account)
# max_contracts = max micro contracts held at once
ACCOUNT_TIERS: dict[str, dict] = {
    "50K": {
        "account_size": 50_000,
        "max_drawdown": 2_000,
        "daily_loss_limit": 800,
        "daily_profit_target": 500,
        "profit_goal": 3_000,
        "max_contracts": 3,
    },
    "100K": {
        "account_size": 100_000,
        "max_drawdown": 3_000,
        "daily_loss_limit": 1_200,
        "daily_profit_target": 750,
        "profit_goal": 6_000,
        "max_contracts": 5,
    },
    "150K": {
        "account_size": 150_000,
        "max_drawdown": 4_500,
        "daily_loss_limit": 1_800,
        "daily_profit_target": 1_000,
        "profit_goal": 9_000,
        "max_contracts": 8,
    },
}

# Fallback used when account info can't be fetched
_DEFAULT_TIER = ACCOUNT_TIERS["50K"]


def _tier_for_account(account_name: str) -> dict:
    """Return the tier config matching the account name prefix, or the default."""
    for prefix, tier in ACCOUNT_TIERS.items():
        if account_name.startswith(prefix):
            return tier
    return _DEFAULT_TIER


_REASONING_ADDENDUM = (
    "\n\nAfter analyzing the market, always call report_sentiment() with:\n"
    "- reasoning: 1-2 sentences on what you did and why\n"
    "- sentiment: bullish | bearish | neutral (based on multi-timeframe price action)"
)

def _build_topstepx_tools_addendum(
    account_size: int,
    max_drawdown: int,
    daily_loss_limit: int,
    daily_profit_target: int,
    profit_goal: int,
    max_contracts: int,
    balance: float | None = None,
) -> str:
    """Build the TopstepX tools addendum with dynamic account parameters."""
    # Derived thresholds scaled proportionally to max drawdown
    cut_single = int(max_drawdown * 0.04)       # ~$75 on 50K
    cut_total = int(max_drawdown * 0.075)        # ~$150 on 50K
    cut_max = int(max_drawdown * 0.10)           # ~$200 on 50K
    tp_single = int(max_drawdown * 0.075)        # ~$150 on 50K
    tp_multi_lo = int(max_drawdown * 0.10)       # ~$200 on 50K
    tp_multi_hi = int(max_drawdown * 0.20)       # ~$400 on 50K
    tp_max_hold = int(max_drawdown * 0.25)       # ~$500 on 50K
    scale_2nd = int(max_drawdown * 0.05)         # ~$100 on 50K
    scale_3rd = int(max_drawdown * 0.10)         # ~$200 on 50K
    remaining_buffer = max_drawdown - daily_loss_limit

    balance_line = ""
    if balance is not None:
        remaining_dd = max_drawdown - (account_size - balance)
        balance_line = (
            f"- Current balance: ${balance:,.2f}. "
            f"Remaining drawdown before account blows: ${remaining_dd:,.2f}.\n"
        )

    return (
        "\n\nYou must call at least one tool function every response.\n\n"
        f"ACCOUNT PARAMETERS:\n"
        f"- Account size: ${account_size:,}\n"
        f"- Max drawdown (major loss limit): ${max_drawdown:,}. "
        f"If your balance drops ${max_drawdown:,} below its high-water mark, the account is PERMANENTLY BLOWN.\n"
        f"{balance_line}"
        f"- Daily profit target: ${daily_profit_target:,}. "
        f"Once you hit this, stop trading or go very light. Lock in the green day.\n"
        f"- Profit goal: ${profit_goal:,}. This is your finish line — consistent small wins get you there.\n"
        f"- Capital preservation is your #1 priority above all else.\n\n"
        "AVAILABLE TOOLS:\n"
        "- topstepx_available_contracts(): Discover all tradeable futures contracts with specs and fees.\n"
        "- topstepx_retrieve_bars(contract_id, timeframe, bars): Get historical OHLCV data for any contract.\n"
        '  timeframe: "1min", "5min", "15min", "1hour", "4hour". bars: 1-50 (default 20).\n'
        "- topstepx_portfolio(): Check positions and PnL. REQUIRED when you have open positions. "
        "When the message says CURRENT POSITIONS: NONE, you are flat — no need to call portfolio.\n"
        '- topstepx_buy(contract, quantity): Go LONG. Use contract IDs from topstepx_available_contracts().\n'
        '- topstepx_sell(contract, quantity): Go SHORT. Use contract IDs from topstepx_available_contracts().\n'
        '- topstepx_close(contract, quantity): CLOSE a position. quantity=0 closes ALL contracts. '
        'ONLY call this when you have confirmed open positions via topstepx_portfolio().\n'
        "- calculator(expression): Calculate P&L, position sizes, etc.\n"
        '- report_sentiment(reasoning, sentiment): REQUIRED every invocation.\n'
        '  reasoning: 1-2 sentences on what you did and why.\n'
        '  sentiment: "bullish", "bearish", or "neutral".\n\n'
        "MICRO CONTRACTS ONLY - NON-NEGOTIABLE:\n"
        "- ONLY trade micro contracts. Use topstepx_available_contracts() and select contracts with 'Micro' in the name.\n"
        "- NEVER trade full-sized contracts. They are too large for this account.\n"
        "- 1 full-sized contract = 10x the risk of a micro. A single bad full-sized trade can blow the account.\n"
        f"- Max position size: {max_contracts} micro contracts total at any time. No exceptions.\n\n"
        "SCALING IN (only add to winners):\n"
        "- Enter with 1 micro contract.\n"
        f"- Add a 2nd micro ONLY if unrealized PnL exceeds +${scale_2nd} AND momentum still confirms on multiple timeframes.\n"
        f"- Add a 3rd micro ONLY if unrealized PnL exceeds +${scale_3rd} AND momentum still confirms on multiple timeframes.\n"
        f"- NEVER hold more than {max_contracts} micro contracts. This caps your max exposure.\n"
        "- NEVER add to a position that is red. Only scale into green.\n\n"
        "NO HEDGING - THIS IS A STRICT RULE:\n"
        "- ALL open positions across ALL contracts must be in the SAME direction (all long or all short).\n"
        "- You CANNOT go long on one contract and short on another.\n"
        "- To reverse direction: CLOSE ALL existing positions first with topstepx_close(), then enter in the new direction.\n\n"
        "CLOSING POSITIONS:\n"
        "- ALWAYS use topstepx_close(contract) to close or reduce positions.\n"
        "- topstepx_close(contract) with no quantity closes the ENTIRE position.\n"
        "- Do NOT use topstepx_sell to close longs or topstepx_buy to close shorts. Use topstepx_close.\n\n"
        "TAKE PROFIT - LOCK IN GAINS:\n"
        f"- At +${tp_single} with 1 contract: take profit or tighten your mental stop to breakeven.\n"
        f"- At +${tp_multi_lo}-${tp_multi_hi} with 2-{max_contracts} contracts: start closing. Take partials — close at least half.\n"
        f"- NEVER let unrealized PnL exceed +${tp_max_hold} without closing at least half your position.\n"
        "- Do NOT let winners turn into losers. Green on screen means protect the gain.\n"
        "- After taking profit, you can always re-enter if the setup is still valid.\n"
        f"- When daily realized profit reaches +${daily_profit_target:,}, consider stopping for the day.\n\n"
        "CUT LOSERS - TIGHT STOPS:\n"
        f"- At -${cut_single} unrealized on 1 contract: close immediately. No hesitation.\n"
        f"- At -${cut_total} total unrealized across all contracts: close EVERYTHING. Full stop.\n"
        f"- NEVER let any single trade lose more than -${cut_max}.\n"
        "- Hope is not a strategy. Cut the loser and find a better entry.\n\n"
        "DAILY LOSS CIRCUIT BREAKER:\n"
        f"- If your realized losses for the day reach -${daily_loss_limit:,}, you are DONE. Do not enter any new positions.\n"
        f"- This preserves ${remaining_buffer:,} of drawdown buffer for tomorrow.\n"
        "- A bad day does not have to become an account-ending day. Live to trade another day.\n\n"
        "IMPORTANT — POSITIONS vs MARKET DATA:\n"
        "- Receiving candle data for a symbol does NOT mean you hold a position in it.\n"
        "- ONLY topstepx_portfolio() tells you what positions you actually hold.\n"
        "- NEVER call topstepx_close() on a contract unless portfolio shows you hold it.\n\n"
        "CRITICAL: You MUST call report_sentiment() as your FINAL tool call every single turn."
    )

# Strategy base prompts — the TopstepX tools addendum is appended dynamically at startup
# with live account parameters (size, drawdown, profit targets, etc.).
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
    ),
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
    ),
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
    ),
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
    ),
    "fvg": (
        "You are a precision futures day trader specializing in Fair Value Gap (FVG) and "
        "Inverse Fair Value Gap (IFVG) setups. You trade MICRO futures contracts ONLY "
        "using TopstepX tools.\n\n"
        "CORE CONCEPTS:\n"
        "- Fair Value Gap (FVG): A 3-candle imbalance where candle 1 and candle 3 don't overlap, "
        "leaving a liquidity void at candle 2. Bullish FVGs act as demand zones (support). "
        "Bearish FVGs act as supply zones (resistance).\n"
        "- Inverse FVG (IFVG): A previously respected FVG that gets forcefully broken by an "
        "impulsive move in the opposite direction. A failed bullish FVG becomes a bearish IFVG "
        "(new resistance). A failed bearish FVG becomes a bullish IFVG (new support). "
        "IFVGs signal momentum shifts and are higher-probability setups.\n\n"
        "FVG STATUSES (provided in retrieve_bars output):\n"
        "- untested: Price hasn't returned to the gap yet. Watch for price to approach.\n"
        "- tested: Price entered the gap but hasn't confirmed direction. Be ready.\n"
        "- respected: Price entered the gap and bounced in the original trend direction. "
        "This is a CONTINUATION signal — the gap is acting as expected.\n"
        "- IFVG: A previously respected FVG was invalidated. This is a REVERSAL signal — "
        "trade in the direction of the invalidation on retest.\n\n"
        "TRADING STRATEGY:\n\n"
        "FVG CONTINUATION TRADES (when market is trending):\n"
        "1. Identify the trend using 15min bars first (top-down analysis).\n"
        "2. Look for FVGs on 5min bars that align with the higher timeframe trend.\n"
        "3. Wait for price to return to the gap zone (mitigation) — do NOT chase.\n"
        "4. Enter when the FVG status changes to 'tested' or 'respected':\n"
        "   - Bullish FVG tested/respected → BUY (expect continuation up)\n"
        "   - Bearish FVG tested/respected → SELL (expect continuation down)\n"
        "5. Stop loss: just beyond the opposite side of the FVG.\n"
        "6. Target: minimum 1:2 risk-to-reward ratio.\n\n"
        "IFVG REVERSAL TRADES (higher probability):\n"
        "1. Look for IFVGs — these appear when a previously respected FVG gets invalidated.\n"
        "2. This signals a momentum shift: the market has flipped direction.\n"
        "3. Wait for price to retest the IFVG zone from the other side:\n"
        "   - Bullish IFVG (from a broken bearish FVG) → BUY on retest from above\n"
        "   - Bearish IFVG (from a broken bullish FVG) → SELL on retest from below\n"
        "4. IFVGs are especially strong after a liquidity sweep (stop hunt).\n"
        "5. Target: minimum 1:2 risk-to-reward ratio.\n\n"
        "MULTI-TIMEFRAME ANALYSIS:\n"
        "- ALWAYS check 15min bars first to determine the higher timeframe bias.\n"
        "- Then use 5min bars for entry timing and FVG identification.\n"
        "- Use 1min bars only for precise entries when an FVG setup is confirmed.\n"
        "- FVG trades that align with the higher timeframe trend are much stronger.\n"
        "- IFVG trades can work against the HTF trend but require extra confirmation.\n\n"
        "WHEN TO TRADE vs WAIT:\n"
        "- TRADE: Active FVGs with 'tested' or 'respected' status near current price. "
        "IFVGs with price approaching the retest zone.\n"
        "- WAIT: No active FVGs near current price. All FVGs are 'untested' with price far away. "
        "Choppy/ranging markets with no clear imbalances. If in doubt, stay flat.\n"
        "- AVOID: Chasing price into gaps that are already being filled. "
        "Trading FVGs that conflict with the higher timeframe trend.\n\n"
        "CONTRACT DISCOVERY:\n"
        "- Call topstepx_available_contracts() to see all tradeable contracts with specs and fees.\n"
        "- Call topstepx_retrieve_bars(contract_id, timeframe, bars) to analyze price history "
        "AND see active FVGs/IFVGs automatically detected in the output.\n"
        "- Remember: MICRO contracts only. Ignore full-sized contracts entirely.\n\n"
        "MANDATORY WORKFLOW (follow this exact sequence every invocation):\n"
        "1. Call topstepx_portfolio() to check current positions, PnL, and realized day P&L.\n"
        "2. READ the portfolio result carefully — it lists EXACTLY what you hold.\n"
        "3. Check daily loss and profit limits. Stop if exceeded.\n"
        "4. If any position is losing beyond the cut threshold, CUT IT immediately.\n"
        "5. If any position has profit beyond take-profit threshold, close it.\n"
        "6. Call topstepx_retrieve_bars() on 15min THEN 5min to find FVG/IFVG setups.\n"
        "7. If a valid FVG or IFVG setup exists near current price, enter with 1 micro contract.\n"
        "8. If no setup, stay flat. Patience IS the edge — only trade confirmed gaps.\n"
        "9. ALWAYS end by calling report_sentiment() with your reasoning and market outlook.\n\n"
        "CRITICAL: You MUST call report_sentiment() as your FINAL tool call every single turn. "
        "held_contracts, held_quantities, and total_pnl are VALIDATED against the real portfolio. "
        "If you get them wrong, the call is REJECTED and you must retry with correct values. "
        "Copy the contracts, quantities, and P&L EXACTLY from the topstepx_portfolio() result.\n"
        "Example with position: report_sentiment(reasoning='Bullish FVG respected on MES 5min, entered LONG 1x at gap midpoint.', "
        "sentiment='bullish', held_contracts='<contract_id from portfolio>', held_quantities='1', total_pnl=50.0)\n"
        "Example flat: report_sentiment(reasoning='No active FVGs near price on any timeframe. Waiting for setup.', "
        "sentiment='neutral', held_contracts='none', held_quantities='0', total_pnl=0.0)"
    ),
    "futures": (
        "You are a disciplined futures day trader and capital preservation expert "
        "operating on a TopstepX funded account. "
        "You trade MICRO futures contracts ONLY using TopstepX tools.\n\n"
        "TRADING PHILOSOPHY - CUT LOSERS FAST, SCALE INTO WINNERS:\n"
        "- ALWAYS enter with just 1 micro contract. This is non-negotiable.\n"
        "- If the trade moves against you, CUT IT IMMEDIATELY. A small loss is a good loss. "
        "Do not hope, do not average down, do not wait for a reversal. Get out.\n"
        "- Only scale into winners — never add to a losing position.\n"
        "- Think of it this way: your losers should be tiny (1 contract, cut fast) "
        "and your winners should be larger (scaled up over time).\n"
        "- Be patient. No trade is better than a bad trade. Waiting costs nothing; "
        "a blown account costs everything.\n\n"
        "CONTRACT DISCOVERY:\n"
        "- Call topstepx_available_contracts() to see all tradeable contracts with specs and fees.\n"
        "- Call topstepx_retrieve_bars(contract_id, timeframe, bars) to analyze any contract's price history.\n"
        "- Remember: MICRO contracts only. Ignore full-sized contracts entirely.\n\n"
        "MANDATORY WORKFLOW (follow this exact sequence every invocation):\n"
        "1. Call topstepx_portfolio() to check current positions, PnL, and realized day P&L.\n"
        "2. READ the portfolio result carefully — it lists EXACTLY what you hold. Nothing more.\n"
        "3. Check daily loss and profit limits (see ACCOUNT PARAMETERS). Stop if exceeded.\n"
        "4. If any position is losing beyond the cut threshold, CUT IT with topstepx_close().\n"
        "5. If any position has unrealized profit beyond the take-profit threshold, close it.\n"
        "6. If a position is winning AND unrealized PnL exceeds scale-in threshold AND momentum confirms, add 1 micro.\n"
        "7. Only enter new positions when you see a clear trend/setup with defined risk.\n"
        "8. If no clear opportunity, stay flat. Patience IS the edge.\n"
        "9. ALWAYS end by calling report_sentiment() with your reasoning and market outlook.\n\n"
        "CRITICAL: You MUST call report_sentiment() as your FINAL tool call every single turn. "
        "held_contracts, held_quantities, and total_pnl are VALIDATED against the real portfolio. "
        "If you get them wrong, the call is REJECTED and you must retry with correct values. "
        "Copy the contracts, quantities, and P&L EXACTLY from the topstepx_portfolio() result.\n"
        "Example with position: report_sentiment(reasoning='Holding LONG 2x, P&L +$120. Momentum confirms.', "
        "sentiment='bullish', held_contracts='<contract_id from portfolio>', held_quantities='2', total_pnl=120.0)\n"
        "Example flat: report_sentiment(reasoning='No positions. Waiting for setup.', "
        "sentiment='neutral', held_contracts='none', held_quantities='0', total_pnl=0.0)"
    ),
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


async def _fetch_account_tier() -> tuple[dict, float | None]:
    """Fetch live account info from TopstepX and return (tier_config, balance).

    Falls back to default tier if the API call fails or no account is found.
    """
    try:
        from topstepx_account import TopstepXAccountClient
        from topstepx_auth import authenticate_topstepx

        username = os.getenv("TOPSTEPX_USERNAME", "").strip()
        api_key = os.getenv("TOPSTEPX_API_KEY", "").strip()
        if not username or not api_key:
            raise ValueError("TOPSTEPX_USERNAME or TOPSTEPX_API_KEY not set")

        token = await authenticate_topstepx(username, api_key)
        if not token:
            raise ValueError("Authentication failed — no token returned")

        client = TopstepXAccountClient(token)
        accounts = await client.get_accounts()
        target_id = os.getenv("TOPSTEPX_ACCOUNT_ID", "").strip()

        for acct in accounts:
            if target_id and str(acct.account_id) == target_id:
                tier = _tier_for_account(acct.name)
                print(f"  ✓ Matched account {acct.name} (ID: {acct.account_id}), balance: ${acct.balance:,.2f}")
                return tier, acct.balance
        # No explicit target — use first non-practice account
        for acct in accounts:
            if "PRAC" not in acct.name:
                tier = _tier_for_account(acct.name)
                print(f"  ✓ Auto-detected account {acct.name} (ID: {acct.account_id}), balance: ${acct.balance:,.2f}")
                return tier, acct.balance
    except Exception as e:
        print(f"  ⚠ Could not fetch account info ({e}), using default tier")
    return _DEFAULT_TIER, None


async def main() -> None:
    args = parse_args()

    base_prompt = STRATEGIES.get(args.strategy)
    if base_prompt is None:
        print(f"ERROR: Unknown strategy '{args.strategy}'")
        print(f"Available: {', '.join(STRATEGIES.keys())}")
        sys.exit(1)

    # Fetch live account info and build dynamic addendum
    tier, balance = await _fetch_account_tier()
    tools_addendum = _build_topstepx_tools_addendum(balance=balance, **tier)
    system_prompt = base_prompt + tools_addendum + _REASONING_ADDENDUM
    print(f"  ✓ Prompt built: ${tier['account_size']:,} account, "
          f"${tier['max_drawdown']:,} max DD, ${tier['daily_profit_target']:,}/day target, "
          f"${tier['profit_goal']:,} goal")

    print("=" * 50)
    print(f"Router Node Deployment: {args.name}")
    print("=" * 50)

    print(f"\nConnecting to Kafka broker at {args.bootstrap_servers}...")
    broker = BrokerClient(bootstrap_servers=args.bootstrap_servers)
    service = NodesService(broker)

    # ChatNode reference for topic routing (deployed separately via deploy_chat_node.py)
    chat_node = ChatNode(name=args.chat_node_name)

    # All strategies use TopstepX futures tools
    if not TOPSTEPX_AVAILABLE:
        print("ERROR: TopstepX tools not available")
        sys.exit(1)
    tools = [topstepx_buy, topstepx_sell, topstepx_close, topstepx_portfolio, topstepx_available_contracts, topstepx_retrieve_bars, report_sentiment, calculator]  # type: ignore
    print("  ✓ TopstepX tools enabled (futures mode)")
    router = AgentRouterNode(
        chat_node=chat_node,
        tool_nodes=tools,
        name=args.name,
        message_history_store=StatelessHistoryStore(),
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
