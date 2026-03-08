"""
TopstepX Trading Tools for Agents

Provides @agent_tool functions that allow AI agents to execute simulated trades
backed by MySQL persistent accounts. Real market prices from the TopstepX RTC
feed drive realistic P&L calculation — only order execution is simulated.

Usage:
    python topstepx_trading_tools.py --bootstrap-servers localhost:9092
"""

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from collections import deque
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv

from calfkit._vendor.pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from calfkit.broker.broker import BrokerClient
from calfkit.models.event_envelope import EventEnvelope
from calfkit.models.tool_context import ToolContext
from calfkit.nodes.base_tool_node import agent_tool
from calfkit.runners.service import NodesService
from sim_account_manager import SimAccountManager
from topstepx_account import TopstepXAccountClient, TopstepXPriceStreamer
from unified_market_connector import FUTURES_PRICE_TOPIC

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ── TopstepX Order Enums ─────────────────────────────────────────

class OrderType:
    """TopstepX order types."""
    LIMIT = 1
    MARKET = 2
    STOP = 3
    STOP_LIMIT = 4


class OrderSide:
    """TopstepX order sides."""
    BUY = 0
    SELL = 1


# ── TopstepX Trading Client (real practice account) ─────────────

class TopstepXTradingClient:
    """Client for executing trades on TopstepX."""

    def __init__(self, jwt_token: str, api_base_url: str = "https://api.topstepx.com"):
        self._account_client = TopstepXAccountClient(jwt_token, api_base_url)
        self._api_base = api_base_url.rstrip('/')
        self._http_client = self._account_client._http_client

    async def get_practice_account_id(self) -> Optional[int]:
        accounts = await self._account_client.get_accounts()
        for account in accounts:
            if "PRAC" in account.name:
                return int(account.account_id)
        return None

    async def place_market_order(self, account_id: int, contract_id: str, side: int, size: int) -> dict:
        url = f"{self._api_base}/api/Order/place"
        payload = {
            "accountId": account_id,
            "contractId": contract_id,
            "type": OrderType.MARKET,
            "side": side,
            "size": size,
        }
        logger.info(f"Placing market order: {payload}")
        try:
            response = await self._http_client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            if not data.get("success"):
                error_msg = data.get("errorMessage", "Unknown error")
                error_code = data.get("errorCode", -1)
                logger.error(f"Order failed: [{error_code}] {error_msg}")
                return {"success": False, "error": error_msg, "errorCode": error_code}
            logger.info(f"Order placed successfully: {data}")
            return data
        except Exception as e:
            logger.error(f"Error placing order: {e}")
            return {"success": False, "error": str(e)}

    async def close_position(self, account_id: int, contract_id: str) -> dict:
        url = f"{self._api_base}/api/Position/closeContract"
        payload = {"accountId": account_id, "contractId": contract_id}
        logger.info(f"Closing full position: {payload}")
        try:
            response = await self._http_client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            if not data.get("success"):
                error_msg = data.get("errorMessage", "Unknown error")
                logger.error(f"Close failed: {error_msg}")
                return {"success": False, "error": error_msg}
            logger.info(f"Position closed: {contract_id}")
            return data
        except Exception as e:
            logger.error(f"Error closing position: {e}")
            return {"success": False, "error": str(e)}

    async def partial_close_position(self, account_id: int, contract_id: str, size: int) -> dict:
        url = f"{self._api_base}/api/Position/partialCloseContract"
        payload = {"accountId": account_id, "contractId": contract_id, "size": size}
        logger.info(f"Partial close position: {payload}")
        try:
            response = await self._http_client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            if not data.get("success"):
                error_msg = data.get("errorMessage", "Unknown error")
                logger.error(f"Partial close failed: {error_msg}")
                return {"success": False, "error": error_msg}
            logger.info(f"Partial close: {size}x {contract_id}")
            return data
        except Exception as e:
            logger.error(f"Error partial closing position: {e}")
            return {"success": False, "error": str(e)}

    async def get_account_summary(self, account_id: int) -> dict:
        accounts = await self._account_client.get_accounts()
        target = None
        for account in accounts:
            if str(account.account_id) == str(account_id):
                target = account
                break
        if target is None:
            for account in accounts:
                if "PRAC" in account.name:
                    target = account
                    logger.info(f"Account {account_id} not found, using {account.account_id} ({account.name})")
                    break
        if target is None:
            return {"error": f"Account {account_id} not found"}
        return {
            "accountId": target.account_id,
            "name": target.name,
            "equity": target.equity,
            "balance": target.balance,
            "canTrade": target.can_trade,
            "positions": [
                {
                    "symbol": pos.symbol,
                    "quantity": pos.quantity,
                    "avgPrice": pos.avg_price,
                    "marketValue": pos.market_value,
                    "unrealizedPnL": pos.unrealized_pnl,
                }
                for pos in target.positions
            ],
        }

    async def close(self):
        await self._account_client.close()


# ── Module-level clients ─────────────────────────────────────────

_sim_manager: Optional[SimAccountManager] = None
_trading_client: Optional[TopstepXTradingClient] = None
_practice_account_id: Optional[int] = None


def _init_client():
    """Initialize TopstepX trading client (for real practice account + RTC prices)."""
    global _trading_client

    if _trading_client is not None:
        return

    jwt_token = os.getenv("TOPSTEPX_JWT_TOKEN")
    if not jwt_token:
        logger.warning("TOPSTEPX_JWT_TOKEN not set - real trading + RTC disabled")
        return

    api_url = os.getenv("TOPSTEPX_API_URL", "https://api.topstepx.com")
    _trading_client = TopstepXTradingClient(jwt_token, api_url)
    logger.info("TopstepX trading client initialized (for RTC + real trading mirror)")


async def _ensure_practice_account(force_refresh: bool = False):
    """Ensure practice account ID is loaded."""
    global _practice_account_id

    if _trading_client is None:
        return None

    if _practice_account_id is None or force_refresh:
        new_id = await _trading_client.get_practice_account_id()
        if new_id:
            if new_id != _practice_account_id:
                logger.info(f"Practice account ID updated: {_practice_account_id} -> {new_id}")
            _practice_account_id = new_id
        else:
            logger.warning("No practice account found")

    return _practice_account_id


async def _mirror_to_practice(action: str, contract: str, quantity: int) -> None:
    """Mirror a simulated trade to the real TopstepX practice account.

    Called only for the designated 'best agent' (controlled by MIRROR_AGENT env var).
    Failures are logged but never block the sim trade.
    """
    if _trading_client is None or _practice_account_id is None:
        return
    try:
        if action == "BUY":
            await _trading_client.place_market_order(
                _practice_account_id, contract, OrderSide.BUY, quantity)
        elif action == "SELL":
            await _trading_client.place_market_order(
                _practice_account_id, contract, OrderSide.SELL, quantity)
        elif action == "CLOSE":
            if quantity == 0:
                await _trading_client.close_position(_practice_account_id, contract)
            else:
                await _trading_client.partial_close_position(_practice_account_id, contract, quantity)
        logger.info(f"MIRROR -> practice: {action} {quantity}x {contract}")
    except Exception as e:
        logger.error(f"Mirror to practice failed: {e}")


# ── Agent activity state ─────────────────────────────────────────

BULLISH_KEYWORDS = {"bullish", "upward", "scaling in", "buy", "long", "momentum"}
BEARISH_KEYWORDS = {"bearish", "downward", "cutting", "sell", "short", "risk off"}
NEUTRAL_KEYWORDS = {"flat", "wait", "no clear", "patience"}

_all_agent_states: dict[str, dict] = {}  # agent_name -> state dict
_all_agent_activities: dict[str, deque] = {}  # agent_name -> activity deque
_agent_seen: set = set()


def _get_agent_state(agent_name: str) -> dict:
    """Get or create per-agent state dict."""
    if agent_name not in _all_agent_states:
        _all_agent_states[agent_name] = {
            "agent_name": agent_name,
            "model": "",
            "logo": "",
            "strategy": "",
            "sentiment": "neutral",
            "last_active": None,
            "latest_reasoning": None,
            "activity": [],
        }
        _all_agent_activities[agent_name] = deque(maxlen=10)
    return _all_agent_states[agent_name]


def _get_agent_activity(agent_name: str) -> deque:
    """Get or create per-agent activity deque."""
    if agent_name not in _all_agent_activities:
        _get_agent_state(agent_name)  # ensures both are created
    return _all_agent_activities[agent_name]


def _extract_sentiment(text: str) -> str:
    """Three-tier sentiment extraction.

    1. Explicit tag (highest priority): ``Sentiment: bullish|bearish|neutral``
    2. Keyword scan (fallback): count bullish/bearish keywords
    3. Returns "neutral" if nothing matches
    """
    # Tier 1: explicit tag
    match = re.search(r"(?i)sentiment:\s*(bullish|bearish|neutral)", text)
    if match:
        return match.group(1).lower()

    # Tier 2: keyword scan
    lower = text.lower()
    bull = sum(1 for kw in BULLISH_KEYWORDS if kw in lower)
    bear = sum(1 for kw in BEARISH_KEYWORDS if kw in lower)
    if bull > bear:
        return "bullish"
    if bear > bull:
        return "bearish"
    return "neutral"


def _infer_sentiment_from_tool_calls(tool_calls: list) -> str:
    """Tier 3: infer sentiment from recent tool call names."""
    names = [tc.tool_name for tc in tool_calls]
    has_buy = any("buy" in n.lower() for n in names)
    has_sell = any("sell" in n.lower() for n in names)
    if has_buy and not has_sell:
        return "bullish"
    if has_sell and not has_buy:
        return "bearish"
    return "neutral"


def _truncate(s: str, max_len: int) -> str:
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "\u2026"


_POSITION_WORDS = re.compile(
    r'\b(long|short|flat|holding|hold|held|positions?|contracts?|MES|unrealized|P&L|pnl|no open|no entries)\b',
    re.IGNORECASE,
)


def _filter_position_hallucinations(text: str) -> str:
    """Strip sentences that mention positions/contracts from LLM text.

    The ground-truth prefix has accurate position data; we only keep
    the strategy/analysis portion of the reasoning.
    """
    sentences = re.split(r'(?<=[.!;])\s+', text)
    cleaned = [s for s in sentences if not _POSITION_WORDS.search(s)]
    return " ".join(cleaned).strip() if cleaned else "Monitoring market conditions."


def _format_tool_call(part: ToolCallPart) -> str:
    try:
        args = part.args_as_dict()
    except Exception:
        args = {}
    if args:
        params = ", ".join(f"{k}={_truncate(json.dumps(v), 80)}" for k, v in args.items())
        return f"{part.tool_name}({params})"
    return f"{part.tool_name}()"


def _process_agent_envelope(envelope: EventEnvelope) -> None:
    """Parse an EventEnvelope and update per-agent state."""
    agent_name = envelope.agent_name or "unknown"
    trace_id = envelope.trace_id
    history_len = len(envelope.message_history) + len(envelope.uncommitted_messages)

    # Dedup
    if trace_id:
        key = (trace_id, history_len)
        if key in _agent_seen:
            return
        _agent_seen.add(key)
        # Prevent unbounded growth
        if len(_agent_seen) > 500:
            _agent_seen.clear()

    # Check both uncommitted messages (newest) and latest in history
    messages_to_check = list(envelope.uncommitted_messages)
    if envelope.latest_message_in_history is not None:
        messages_to_check.append(envelope.latest_message_in_history)

    if not messages_to_check:
        return

    state = _get_agent_state(agent_name)
    activity = _get_agent_activity(agent_name)
    now = datetime.now().strftime("%H:%M:%S")
    state["last_active"] = datetime.now().isoformat()

    for msg in messages_to_check:
        if isinstance(msg, ModelResponse):
            tool_calls = [p for p in msg.parts if isinstance(p, ToolCallPart)]
            text_parts = [p.content for p in msg.parts if isinstance(p, TextPart)]

            if tool_calls:
                lines = [_format_tool_call(tc) for tc in tool_calls]
                activity.append({"time": now, "kind": "TOOL CALL", "details": "\n".join(lines)})
            if text_parts:
                text = " ".join(text_parts)
                # Do NOT update latest_reasoning or sentiment from raw LLM text —
                # it hallucinates positions and contradicts report_sentiment.
                # Dashboard state is only set by report_sentiment and portfolio
                # tool returns (ground-truth paths).
                logger.info(f"[{agent_name}] Raw LLM text (ignored for dashboard): {_truncate(text, 150)}")
                if not tool_calls:
                    activity.append({"time": now, "kind": "RESPONSE", "details": _truncate(text, 300)})
            elif tool_calls:
                # Tier 3: no text at all — infer sentiment from tool calls
                sentiment = _infer_sentiment_from_tool_calls(tool_calls)
                state["sentiment"] = sentiment
                tool_names = ", ".join(tc.tool_name for tc in tool_calls)
                logger.info(f"[{agent_name}] Sentiment inferred from tool calls: {sentiment} | Tools: {tool_names}")

        elif isinstance(msg, ModelRequest):
            tool_returns = [p for p in msg.parts if isinstance(p, ToolReturnPart)]
            if tool_returns:
                lines = [
                    f"{tr.tool_name} -> {_truncate(tr.model_response_str(), 200)}"
                    for tr in tool_returns
                ]
                activity.append({"time": now, "kind": "TOOL RESULT", "details": "\n".join(lines)})
                # Use portfolio tool results to keep dashboard positions accurate
                for tr in tool_returns:
                    if tr.tool_name == "topstepx_portfolio":
                        result = tr.model_response_str()
                        # Extract the "YOU HOLD: ..." line as ground-truth
                        for line in result.split("\n"):
                            if line.startswith("YOU HOLD:"):
                                portfolio_info = line.strip()
                                existing = state.get("latest_reasoning") or ""
                                # Replace any old [LONG/SHORT/Flat] prefix
                                existing = re.sub(r"^\[.*?\]\s*", "", existing)
                                existing = _filter_position_hallucinations(existing)
                                state["latest_reasoning"] = f"[{portfolio_info}] {existing}"
                                break

    state["activity"] = list(activity)


def get_all_agents_state() -> dict:
    """Return state for all agents. {agent_name: state_dict, ...}"""
    return {name: dict(state) for name, state in _all_agent_states.items()}


# ── Agent Tools ──────────────────────────────────────────────────


@agent_tool
async def topstepx_buy(
    ctx: ToolContext,
    contract: str,
    quantity: int,
) -> str:
    """Buy futures contracts.

    Args:
        contract: Contract ID (e.g., "CON.F.US.MES.H26" for Micro E-mini S&P)
        quantity: Number of contracts to buy (must be positive integer)

    Returns:
        Result message
    """
    if _sim_manager is None:
        return "❌ Simulated trading not initialized"

    if quantity <= 0:
        return "❌ Quantity must be positive"

    agent_name = ctx.agent_name or os.getenv("AGENT_NAME", "default")
    logger.info(f"🔵 SIM BUY ORDER: {agent_name} {quantity}x {contract}")
    result = await _sim_manager.execute_buy(agent_name, contract, quantity)

    if result.get("success"):
        price = result["fill_price"]
        logger.info(f"✅ SIM BUY FILLED: {quantity}x {contract} @ ${price:,.2f}")
        # Mirror to real practice account if this is the designated agent
        mirror_agent = os.getenv("MIRROR_AGENT", "")
        if mirror_agent and agent_name == mirror_agent:
            await _mirror_to_practice("BUY", contract, quantity)
        return (
            f"✓ BUY order filled\n"
            f"  Contract: {contract}\n"
            f"  Quantity: {quantity}\n"
            f"  Fill Price: ${price:,.2f}\n"
            f"  Account: {agent_name}"
        )
    else:
        error = result.get("error", "Unknown error")
        logger.error(f"❌ SIM BUY REJECTED: {quantity}x {contract} | {error}")
        return f"❌ Order failed: {error}"


@agent_tool
async def topstepx_sell(
    ctx: ToolContext,
    contract: str,
    quantity: int,
) -> str:
    """Sell futures contracts.

    Args:
        contract: Contract ID (e.g., "CON.F.US.MES.H26" for Micro E-mini S&P)
        quantity: Number of contracts to sell (must be positive integer)

    Returns:
        Result message
    """
    if _sim_manager is None:
        return "❌ Simulated trading not initialized"

    if quantity <= 0:
        return "❌ Quantity must be positive"

    agent_name = ctx.agent_name or os.getenv("AGENT_NAME", "default")
    logger.info(f"🔴 SIM SELL ORDER: {agent_name} {quantity}x {contract}")
    result = await _sim_manager.execute_sell(agent_name, contract, quantity)

    if result.get("success"):
        price = result["fill_price"]
        logger.info(f"✅ SIM SELL FILLED: {quantity}x {contract} @ ${price:,.2f}")
        mirror_agent = os.getenv("MIRROR_AGENT", "")
        if mirror_agent and agent_name == mirror_agent:
            await _mirror_to_practice("SELL", contract, quantity)
        return (
            f"✓ SELL order filled\n"
            f"  Contract: {contract}\n"
            f"  Quantity: {quantity}\n"
            f"  Fill Price: ${price:,.2f}\n"
            f"  Account: {agent_name}"
        )
    else:
        error = result.get("error", "Unknown error")
        logger.error(f"❌ SIM SELL REJECTED: {quantity}x {contract} | {error}")
        return f"❌ Order failed: {error}"


@agent_tool
async def topstepx_close(
    ctx: ToolContext,
    contract: str,
    quantity: int = 0,
) -> str:
    """Close (or partially close) an open position. USE THIS to take profit or cut losses.

    If quantity is 0 or >= position size, closes the ENTIRE position.
    If quantity is < position size, partially closes that many contracts.

    Args:
        contract: Contract ID (e.g., "CON.F.US.MES.H26")
        quantity: Number of contracts to close. 0 = close all.

    Returns:
        Result message
    """
    if _sim_manager is None:
        return "❌ Simulated trading not initialized"

    agent_name = ctx.agent_name or os.getenv("AGENT_NAME", "default")
    logger.info(f"🔶 SIM CLOSE: {agent_name} {contract} qty={quantity}")
    result = await _sim_manager.execute_close(agent_name, contract, quantity)

    if result.get("success"):
        price = result["fill_price"]
        pnl = result["realized_pnl"]
        qty_closed = result["quantity_closed"]
        logger.info(f"✅ SIM CLOSE: {qty_closed}x {contract} @ ${price:,.2f} | PnL: ${pnl:+,.2f}")
        mirror_agent = os.getenv("MIRROR_AGENT", "")
        if mirror_agent and agent_name == mirror_agent:
            await _mirror_to_practice("CLOSE", contract, quantity)
        lines = [
            f"✓ Position CLOSED",
            f"  Contract: {contract}",
            f"  Quantity closed: {qty_closed}",
            f"  Fill Price: ${price:,.2f}",
            f"  Realized P&L: ${pnl:+,.2f}",
            f"  New Balance: ${result['new_balance']:,.2f}",
        ]
        if result.get("blown"):
            lines.append(f"  ⚠️ {result['warning']}")
        return "\n".join(lines)
    else:
        error = result.get("error", "Unknown error")
        if "ZERO open positions" in error:
            return "STOP: You have ZERO open positions. There is nothing to close. Do NOT call topstepx_close."
        if "do NOT hold" in error:
            return f"STOP: {error}. Do NOT close what you don't have."
        logger.error(f"❌ SIM CLOSE FAILED: {contract} | {error}")
        return f"❌ Close failed: {error}"


_sentiment_caches: dict[str, dict] = {}  # agent_name -> {"time": float}
_portfolio_caches: dict[str, dict] = {}  # agent_name -> cache dict


@agent_tool
async def report_sentiment(
    ctx: ToolContext,
    reasoning: str = "",
    sentiment: str = "neutral",
    **kwargs,
) -> str:
    """Report your current market reasoning and sentiment assessment.

    Args:
        reasoning: 1-2 sentences explaining what you did (or chose not to do) and why.
        sentiment: Your market outlook: "bullish", "bearish", or "neutral".

    Returns:
        Confirmation message
    """
    import time as _time

    agent_name = ctx.agent_name or os.getenv("AGENT_NAME", "default")

    # ── Rescue values from wrong arg names the LLM may use ──
    if not reasoning and kwargs:
        reasoning = str(next(iter(kwargs.values()), ""))
    if sentiment == "neutral" and kwargs:
        for v in kwargs.values():
            if isinstance(v, str) and v.strip().lower() in ("bullish", "bearish"):
                sentiment = v.strip().lower()
                break

    # ── Per-agent caches ──
    now = _time.time()
    s_cache = _sentiment_caches.setdefault(agent_name, {"time": 0.0})
    p_cache = _portfolio_caches.setdefault(agent_name, {"result": None, "time": 0.0, "has_positions": False})

    # ── Throttle when flat: only update once per candle cycle (60s) ──
    if not p_cache.get("has_positions") and (now - s_cache["time"]) < 55.0:
        return "Recorded. STOP — do not call any more tools this turn."
    s_cache["time"] = now

    # ── Use cached portfolio state (already fetched by topstepx_portfolio)
    if p_cache.get("has_positions"):
        portfolio_prefix = p_cache.get("prefix", "[Unknown] ")
    else:
        portfolio_prefix = "[Flat] "

    # Fetch fresh positions from sim manager when we have positions
    if p_cache.get("has_positions") and _sim_manager:
        try:
            positions = await _sim_manager.get_positions(agent_name)
            if positions:
                parts = []
                for pos in positions:
                    qty = pos["quantity"]
                    direction = "LONG" if qty > 0 else "SHORT"
                    pnl = pos.get("unrealizedPnL", 0.0)
                    parts.append(f"{direction} {abs(qty)}x {pos['symbol']} P&L: ${pnl:+,.2f}")
                portfolio_prefix = f"[{', '.join(parts)}] "
            else:
                portfolio_prefix = "[Flat] "
                p_cache["has_positions"] = False
        except Exception:
            pass

    sentiment_lower = sentiment.strip().lower()
    if sentiment_lower not in ("bullish", "bearish", "neutral"):
        sentiment_lower = "neutral"

    # ── Update per-agent dashboard state with ground-truth + filtered reasoning ──
    state = _get_agent_state(agent_name)
    display_reasoning = _filter_position_hallucinations(reasoning)
    state["latest_reasoning"] = portfolio_prefix + display_reasoning
    state["sentiment"] = sentiment_lower
    state["last_active"] = datetime.now().isoformat()

    logger.info(f"[{agent_name}] Sentiment ACCEPTED: {sentiment_lower} | {portfolio_prefix}| Display: {_truncate(display_reasoning, 150)}")
    return f"Recorded sentiment: {sentiment_lower}"


@agent_tool
async def topstepx_portfolio(ctx: ToolContext) -> str:
    """Get portfolio status with real-time P&L.

    Returns:
        Portfolio summary with live position data
    """
    if _sim_manager is None:
        return "❌ Simulated trading not initialized"

    agent_name = ctx.agent_name or os.getenv("AGENT_NAME", "default")

    # Per-agent cache: when flat, return cached result for 30s to avoid spamming DB
    import time as _time
    now = _time.time()
    cache = _portfolio_caches.setdefault(agent_name, {"result": None, "time": 0.0, "has_positions": False})
    if (
        cache["result"] is not None
        and not cache["has_positions"]
        and (now - cache["time"]) < 30.0
    ):
        logger.debug(f"📊 PORTFOLIO (cached flat) for {agent_name}: returning cached result")
        return cache["result"]

    logger.info(f"📊 CHECKING PORTFOLIO STATUS for {agent_name}")

    summary = await _sim_manager.get_portfolio(agent_name)

    if "error" in summary:
        return f"❌ {summary['error']}"

    positions = summary.get("positions", [])
    balance = summary["balance"]

    if not positions:
        logger.info(f"💼 PORTFOLIO: No open positions | Balance: ${balance:,.2f}")
        state = _get_agent_state(agent_name)
        state["last_active"] = datetime.now().isoformat()
        result = (
            f"YOU HOLD: nothing — 0 open positions.\n"
            f"Balance: ${balance:,.2f} | Equity: ${balance:,.2f}\n"
            f"---\nNOW call report_sentiment(reasoning=<your analysis>, sentiment=<bullish|bearish|neutral>)"
        )
        cache.update(result=result, time=now, has_positions=False)
        return result

    total_pnl = 0.0
    hold_parts = []

    for pos in positions:
        qty = pos["quantity"]
        direction = "LONG" if qty > 0 else "SHORT"
        abs_qty = abs(int(qty))
        avg = pos["avgPrice"]
        pnl = pos["unrealizedPnL"]
        total_pnl += pnl

        hold_parts.append(f"{direction} {abs_qty}x {pos['symbol']} (P&L: ${pnl:+,.2f})")
        logger.info(f"💼 POSITION: {pos['symbol']} {direction} {abs_qty} @ ${avg:,.2f} | P&L: ${pnl:+,.2f}")

    equity = balance + total_pnl

    lines = [
        f"YOU HOLD: {', '.join(hold_parts)}",
        f"Total unrealized P&L: ${total_pnl:+,.2f}",
        f"Balance: ${balance:,.2f} | Equity: ${equity:,.2f}",
    ]

    if total_pnl <= -200:
        lines.append(f"WARNING: You are losing ${total_pnl:,.2f} — close losing positions NOW.")
    elif total_pnl >= 300:
        lines.append(f"You have ${total_pnl:+,.2f} profit — take profit NOW with topstepx_close().")

    lines.append(
        f"---\nNOW call report_sentiment(reasoning=<your analysis>, sentiment=<bullish|bearish|neutral>)"
    )

    state = _get_agent_state(agent_name)
    state["last_active"] = datetime.now().isoformat()
    result = "\n".join(lines)
    cache.update(result=result, time=now, has_positions=True,
                 prefix=f"[{', '.join(hold_parts)}] ")
    return result


# ── Main service ─────────────────────────────────────────────────


async def main():
    """Main entry point - deploy TopstepX trading tools."""
    global _sim_manager

    parser = argparse.ArgumentParser(description="Deploy TopstepX trading tools")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap servers",
    )
    args = parser.parse_args()

    # ── Initialize MySQL-backed simulated accounts ────────────
    _sim_manager = SimAccountManager()
    await _sim_manager.initialize(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "trading"),
        password=os.getenv("MYSQL_PASSWORD", "trading_pass"),
        db=os.getenv("MYSQL_DATABASE", "trading_arena"),
    )

    # Pre-create accounts for all configured agents
    agent_name = os.getenv("AGENT_NAME", "futures-trader")
    all_agents = {agent_name}
    # Read from agents.yml if available
    agents_yml = os.path.join(os.path.dirname(__file__), "agents.yml")
    agents_cfg = None
    try:
        import yaml
        with open(agents_yml) as f:
            agents_cfg = yaml.safe_load(f)
        if agents_cfg and agents_cfg.get("agents"):
            all_agents |= set(agents_cfg["agents"].keys())
            logger.info(f"Loaded {len(agents_cfg['agents'])} agents from agents.yml")
    except Exception:
        pass  # Fall back to AGENT_NAME / AGENT_NAMES env vars
    # Also support AGENT_NAMES env var as override
    agent_names_csv = os.getenv("AGENT_NAMES", "")
    all_agents |= {n.strip() for n in agent_names_csv.split(",") if n.strip()}
    for name in sorted(all_agents):
        await _sim_manager.get_or_create_account(name)
        logger.info(f"Sim account ready for agent: {name}")

    # Pre-populate dashboard agent panels from agents.yml config
    if agents_cfg and agents_cfg.get("agents") and agents_cfg.get("models"):
        for name, acfg in agents_cfg["agents"].items():
            state = _get_agent_state(name)
            model_key = acfg.get("model", "")
            model_cfg = agents_cfg["models"].get(model_key, {})
            state["model"] = model_cfg.get("model_id", model_key)
            state["strategy"] = acfg.get("strategy", "")
            state["logo"] = model_cfg.get("logo", "openai")
            logger.info(f"Pre-populated dashboard panel for agent: {name} (model={state['model']}, strategy={state['strategy']})")

    # Start background MLL monitor + daily reset scheduler
    bg_tasks = await _sim_manager.start_background_tasks()

    # ── Authenticate with TopstepX (for RTC prices + real trade mirroring) ──
    if not os.getenv("TOPSTEPX_JWT_TOKEN"):
        username = os.getenv("TOPSTEPX_USERNAME")
        api_key = os.getenv("TOPSTEPX_API_KEY")
        if username and api_key:
            from topstepx_auth import authenticate_topstepx
            logger.info("No JWT token found, authenticating with API key...")
            jwt_token = await authenticate_topstepx(
                username, api_key,
                os.getenv("TOPSTEPX_ENVIRONMENT", "demo"),
                os.getenv("TOPSTEPX_API_URL"),
            )
            if not jwt_token:
                logger.warning("TopstepX authentication failed — RTC prices + trade mirroring disabled")
            else:
                os.environ["TOPSTEPX_JWT_TOKEN"] = jwt_token
                logger.info("Authenticated successfully — JWT token obtained")
        else:
            logger.info("TopstepX credentials not set — RTC prices + trade mirroring disabled")

    # Initialize TopstepX client for RTC prices, practice account stats, and trade mirroring
    _init_client()
    _web_client = None
    _dashboard_client = None
    if _trading_client:
        await _ensure_practice_account()
        if _practice_account_id:
            mirror_agent = os.getenv("MIRROR_AGENT", "")
            if mirror_agent:
                logger.info(f"Trade mirroring enabled: {mirror_agent} -> practice account {_practice_account_id}")
            else:
                logger.info(f"Practice account {_practice_account_id} found (set MIRROR_AGENT to enable mirroring)")
        else:
            logger.warning("No practice account found — trade mirroring disabled")

        # Initialize web clients for practice account stats
        jwt_token_val = os.getenv("TOPSTEPX_JWT_TOKEN", "")
        if jwt_token_val:
            from topstepx_web_client import TopstepDashboardClient, TopstepXWebClient
            _web_client = TopstepXWebClient(jwt_token_val)
            logger.info("TopstepX web client initialized")

            dash_refresh = os.getenv("TOPSTEP_REFRESH_TOKEN", "").strip()
            if dash_refresh:
                _dashboard_client = TopstepDashboardClient(refresh_token=dash_refresh)
                logger.info("Topstep dashboard client initialized")
            else:
                logger.info("TOPSTEP_REFRESH_TOKEN not set — dashboard balance history disabled")

    # Start real-time price streaming via SignalR gateway
    price_streamer = None
    jwt_token = os.getenv("TOPSTEPX_JWT_TOKEN", "")
    stream_symbols_str = os.getenv("TOPSTEPX_STREAM_SYMBOLS", "").strip()
    if stream_symbols_str:
        stream_symbols = [s.strip() for s in stream_symbols_str.split(",") if s.strip()]
    elif _trading_client and _practice_account_id:
        positions = await _trading_client._account_client.get_positions(_practice_account_id)
        stream_symbols = list({pos.symbol for pos in positions})
    else:
        stream_symbols = []

    if jwt_token and stream_symbols:
        ws_base = os.getenv("TOPSTEPX_API_URL", "https://api.topstepx.com").replace("api.", "rtc.")
        price_streamer = TopstepXPriceStreamer(
            jwt_token, stream_symbols, ws_base=ws_base,
            account_client=_trading_client._account_client if _trading_client else None,
            account_id=_practice_account_id,
        )
        await price_streamer.start()
        logger.info(f"RTC price streamer started for: {stream_symbols}")

    print("=" * 60)
    print("TopstepX Trading Arena (Simulated Accounts + RTC Prices)")
    print("=" * 60)

    # Initialize Kafka
    print(f"\nConnecting to Kafka at {args.bootstrap_servers}...")
    broker = BrokerClient(bootstrap_servers=args.bootstrap_servers)
    service = NodesService(broker)

    # Subscribe to futures price updates from market-connector (backup to RTC)
    @broker.subscriber(FUTURES_PRICE_TOPIC, group_id="topstepx-trading-tools")
    async def handle_futures_price(message: dict) -> None:
        contract_id = message.get("contract_id")
        price = message.get("price")
        if contract_id and price is not None:
            TopstepXAccountClient.update_market_price(contract_id, float(price))

    # Subscribe to agent output for dashboard activity panel
    @broker.subscriber("agent_router.output", group_id="dashboard-agent-viewer")
    async def handle_agent_output(envelope: EventEnvelope) -> None:
        _process_agent_envelope(envelope)

    # Register tools
    print("\nRegistering trading tools:")
    tools = [topstepx_buy, topstepx_sell, topstepx_close, topstepx_portfolio, report_sentiment]
    for tool in tools:
        service.register_node(tool)
        print(f"  ✓ {tool.tool_schema.name} - {tool.tool_schema.description}")

    print(f"\n✓ Simulated trading enabled for agent: {agent_name}")
    if _practice_account_id:
        print(f"✓ RTC prices streaming | Practice account: {_practice_account_id}")
    print("\nPress Ctrl+C to stop...")

    # ── Start web dashboard alongside Kafka service ────────────
    dashboard_port = int(os.getenv("DASHBOARD_PORT", "8080"))
    try:
        import socket

        import uvicorn
        from topstepx_web_dashboard import create_app

        def _set_account_id(new_id: int):
            global _practice_account_id
            if new_id != _practice_account_id:
                logger.info(f"Account ID refreshed: {_practice_account_id} -> {new_id}")
                _practice_account_id = new_id

        dashboard_app = create_app(
            sim_manager=_sim_manager,
            get_all_agents_state=get_all_agents_state,
            agent_name=agent_name,
            mirror_agent=os.getenv("MIRROR_AGENT", ""),
            trading_client=_trading_client,
            get_account_id=lambda: _practice_account_id,
            set_account_id=_set_account_id,
            web_client=_web_client,
            dashboard_client=_dashboard_client,
        )

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        for attempt in range(10):
            try:
                sock.bind(("0.0.0.0", dashboard_port))
                break
            except OSError:
                logger.info(f"Port {dashboard_port} busy, retrying ({attempt + 1}/10)...")
                await asyncio.sleep(2)
        else:
            logger.error(f"Could not bind to port {dashboard_port} after 10 attempts")
            sock.close()
            await service.run()
            return
        sock.listen(128)

        uvicorn_config = uvicorn.Config(
            dashboard_app,
            log_level="info",
            access_log=False,
            ws_ping_interval=20,
            ws_ping_timeout=60,
        )
        uvicorn_server = uvicorn.Server(uvicorn_config)
        print(f"\n✓ Web dashboard available at http://0.0.0.0:{dashboard_port}")

        await asyncio.gather(
            service.run(),
            uvicorn_server.serve(sockets=[sock]),
        )
    except ImportError:
        logger.warning("fastapi/uvicorn not installed — running without web dashboard")
        await service.run()
    finally:
        for t in bg_tasks:
            t.cancel()
        if price_streamer:
            await price_streamer.stop()
        if _trading_client:
            await _trading_client.close()
        if _web_client:
            await _web_client.close()
        if _dashboard_client:
            await _dashboard_client.close()
        if _sim_manager:
            await _sim_manager.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTopstepX trading tools stopped.")
