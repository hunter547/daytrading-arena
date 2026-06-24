"""
TopstepX Trading Tools for Agents

Provides @agent_tool functions that allow AI agents to execute simulated trades
backed by MySQL persistent accounts. Real market prices from the TopstepX RTC
feed drive realistic P&L calculation — only order execution is simulated.

A single "promoted" agent can have its sim trades mirrored to a real TopstepX
account (practice, combine, or funded) via PROMOTED_AGENT + TOPSTEPX_ACCOUNT_ID.

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
import time as _time
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

    async def get_account_id(self, account_id: Optional[int] = None) -> Optional[int]:
        """Get account ID — either the specified one or auto-detect practice.

        Args:
            account_id: Specific account ID to validate, or None to auto-detect

        Returns:
            Validated account ID, or None if not found
        """
        if account_id is not None:
            # Validate the account exists and is accessible
            accounts = await self._account_client.get_accounts()
            for account in accounts:
                if int(account.account_id) == account_id:
                    logger.info(f"Validated TopstepX account: {account.name} (ID: {account_id})")
                    return account_id
            logger.warning(f"Account {account_id} not found — falling back to practice account")
        return await self.get_practice_account_id()

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
        accounts = await self._account_client.get_accounts(only_active=True)
        target = None
        for account in accounts:
            if str(account.account_id) == str(account_id):
                target = account
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
_live_account_id: Optional[int] = None  # TopstepX account (practice, combine, or funded)
_price_streamer: Optional[TopstepXPriceStreamer] = None
_promoted_agent: str = ""  # Agent name whose trades mirror to the live TopstepX account
_nt_copytrader = None      # NinjaTraderCopyTrader — mirrors copy agent's trades to NinjaTrader
_nt_copy_agent: str = ""   # Agent name whose trades are copy-traded to NinjaTrader


def _init_client():
    """Initialize TopstepX trading client (for real account + RTC prices)."""
    global _trading_client

    if _trading_client is not None:
        return

    jwt_token = os.getenv("TOPSTEPX_JWT_TOKEN")
    if not jwt_token:
        logger.warning("TOPSTEPX_JWT_TOKEN not set - real trading + RTC disabled")
        return

    api_url = os.getenv("TOPSTEPX_API_URL", "https://api.topstepx.com")
    _trading_client = TopstepXTradingClient(jwt_token, api_url)
    logger.info("TopstepX trading client initialized (for RTC + real trading)")


async def _ensure_live_account(force_refresh: bool = False):
    """Ensure TopstepX live account ID is loaded.

    Priority:
    1. TOPSTEPX_ACCOUNT_ID env var (combine/funded/practice — explicit)
    2. Auto-detect practice account (PRAC-*)
    """
    global _live_account_id

    if _trading_client is None:
        return None

    if _live_account_id is None or force_refresh:
        explicit_id = os.getenv("TOPSTEPX_ACCOUNT_ID", "").strip()
        requested_id = int(explicit_id) if explicit_id else None
        new_id = await _trading_client.get_account_id(requested_id)
        if new_id:
            if new_id != _live_account_id:
                logger.info(f"TopstepX live account ID: {_live_account_id} -> {new_id}")
            _live_account_id = new_id
        else:
            logger.warning("No TopstepX account found")

    return _live_account_id


async def _mirror_to_live(action: str, contract: str, quantity: int, agent_name: str) -> dict:
    """Mirror a simulated trade to the real TopstepX account.

    Called only for the promoted agent. Returns the API result dict.
    Failures are logged and recorded in the agent's activity feed but never
    block the sim trade.
    """
    if _trading_client is None or _live_account_id is None:
        return {"success": False, "error": "No live account"}

    result = {}
    try:
        if action == "BUY":
            result = await _trading_client.place_market_order(
                _live_account_id, contract, OrderSide.BUY, quantity)
        elif action == "SELL":
            result = await _trading_client.place_market_order(
                _live_account_id, contract, OrderSide.SELL, quantity)
        elif action == "CLOSE":
            if quantity == 0:
                result = await _trading_client.close_position(_live_account_id, contract)
            else:
                result = await _trading_client.partial_close_position(_live_account_id, contract, quantity)

        if result.get("success"):
            logger.info(f"LIVE MIRROR -> account {_live_account_id}: {action} {quantity}x {contract}")
            activity = _get_agent_activity(agent_name)
            activity.append({
                "ts": datetime.now().isoformat(),
                "type": "LIVE_ORDER",
                "msg": f"LIVE {action} {quantity}x {contract} — filled on TopstepX account {_live_account_id}",
            })
        else:
            error = result.get("error", "Unknown error")
            logger.error(f"LIVE MIRROR FAILED: {action} {quantity}x {contract} — {error}")
            activity = _get_agent_activity(agent_name)
            activity.append({
                "ts": datetime.now().isoformat(),
                "type": "LIVE_ORDER_FAIL",
                "msg": f"LIVE {action} FAILED: {contract} — {error}",
            })
    except Exception as e:
        logger.error(f"LIVE MIRROR ERROR: {action} {quantity}x {contract} — {e}")
        activity = _get_agent_activity(agent_name)
        activity.append({
            "ts": datetime.now().isoformat(),
            "type": "LIVE_ORDER_FAIL",
            "msg": f"LIVE {action} ERROR: {contract} — {e}",
        })
        result = {"success": False, "error": str(e)}

    return result


async def _mirror_to_ninjatrader(action: str, contract: str, quantity: int, agent_name: str) -> dict:
    """Copy a simulated trade to the real NinjaTrader account via the bridge.

    Called only for the copy agent. Returns the adapter result dict. Failures are
    logged and recorded in the agent's activity feed but never block the sim trade.
    """
    if _nt_copytrader is None:
        return {"success": False, "error": "No NinjaTrader copy-trader"}

    result = {}
    try:
        if action == "BUY":
            result = await _nt_copytrader.mirror_buy(contract, quantity)
        elif action == "SELL":
            result = await _nt_copytrader.mirror_sell(contract, quantity)
        elif action == "CLOSE":
            result = await _nt_copytrader.mirror_close(contract, quantity)

        instrument = result.get("instrument", contract)
        if result.get("success"):
            logger.info(f"NT COPY -> {_nt_copytrader.account}: {action} {quantity}x {instrument}")
            activity = _get_agent_activity(agent_name)
            activity.append({
                "ts": datetime.now().isoformat(),
                "type": "NT_ORDER",
                "msg": f"COPY {action} {quantity}x {instrument} — sent to NinjaTrader {_nt_copytrader.account}",
            })
        else:
            error = result.get("error", "Unknown error")
            logger.error(f"NT COPY FAILED: {action} {quantity}x {contract} — {error}")
            activity = _get_agent_activity(agent_name)
            activity.append({
                "ts": datetime.now().isoformat(),
                "type": "NT_ORDER_FAIL",
                "msg": f"COPY {action} FAILED: {contract} — {error}",
            })
    except Exception as e:
        logger.error(f"NT COPY ERROR: {action} {quantity}x {contract} — {e}")
        activity = _get_agent_activity(agent_name)
        activity.append({
            "ts": datetime.now().isoformat(),
            "type": "NT_ORDER_FAIL",
            "msg": f"COPY {action} ERROR: {contract} — {e}",
        })
        result = {"success": False, "error": str(e)}

    return result


async def _check_live_hedging(agent_name: str, side: str) -> Optional[str]:
    """Check real TopstepX positions for hedging conflicts.

    When the promoted agent tries to BUY, reject if the live account has shorts.
    When the promoted agent tries to SELL, reject if the live account has longs.

    Returns an error message if blocked, or None if OK.
    """
    if not _promoted_agent or agent_name != _promoted_agent:
        return None
    if _trading_client is None or _live_account_id is None:
        return None

    try:
        positions = await _trading_client._account_client.get_positions(_live_account_id)
        for pos in positions:
            if side == "BUY" and pos.quantity < 0:
                return (
                    f"BLOCKED: Live TopstepX account has SHORT {abs(pos.quantity)}x {pos.symbol}. "
                    f"Close that position on TopstepX first."
                )
            elif side == "SELL" and pos.quantity > 0:
                return (
                    f"BLOCKED: Live TopstepX account has LONG {pos.quantity}x {pos.symbol}. "
                    f"Close that position on TopstepX first."
                )
    except Exception as e:
        logger.error(f"Failed to check live positions for hedging: {e}")
        # Fail open — don't block the sim trade if we can't check
    return None


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
    r'\b(long|short|flat|holding|hold|held|positions?|contracts?|unrealized|P&L|pnl|no open|no entries)\b',
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


# ── Blown account tracking ───────────────────────────────────────
_blown_agents: dict[str, float] = {}  # agent_name -> timestamp when marked blown
_BLOWN_CACHE_TTL = 60.0  # re-check DB every 60s so resets are picked up

async def _check_blown(agent_name: str) -> bool:
    """Check if agent's account is blown. Caches with TTL to avoid repeated DB hits."""
    cached_at = _blown_agents.get(agent_name)
    if cached_at is not None and (_time.monotonic() - cached_at) < _BLOWN_CACHE_TTL:
        return True
    if _sim_manager is None:
        return False
    portfolio = await _sim_manager.get_portfolio(agent_name)
    if portfolio.get("blown"):
        _blown_agents[agent_name] = _time.monotonic()
        return True
    # Account is not blown (possibly reset) — clear stale cache entry
    _blown_agents.pop(agent_name, None)
    return False

_reset_agents: set[str] = set()  # agents that were just reset — notify on next tool call

def clear_blown_cache(agent_name: str) -> None:
    """Clear blown cache for an agent (called on account reset)."""
    _blown_agents.pop(agent_name, None)
    _reset_agents.add(agent_name)

def _consume_reset_notice(agent_name: str) -> str:
    """If the agent was recently reset, return a one-time notice and clear the flag."""
    if agent_name in _reset_agents:
        _reset_agents.discard(agent_name)
        return (
            "\n\n🔄 ACCOUNT RESET NOTICE: Your account has been reset to $150,000. "
            "Trading is FULLY ENABLED. You are NOT blown. "
            "Learn from prior mistakes — manage risk carefully. "
            "Resume trading normally."
        )
    return ""

_BLOWN_MSG = "⛔ ACCOUNT BLOWN — trading is permanently disabled. Do NOT call any more tools."

# ── Evaluation limit tracking ────────────────────────────────────
_dpl_agents: dict[str, float] = {}  # agent_name -> monotonic timestamp when DPL was hit
_pg_agents: dict[str, float] = {}   # agent_name -> monotonic timestamp when PG was hit
_EVAL_CACHE_TTL = 60.0  # re-check DB every 60s so daily resets are picked up

_DPL_MSG = (
    "⛔ DAILY PROFIT LIMIT REACHED — you have hit the maximum allowed profit for today. "
    "Trading is DISABLED for the rest of this session. Do NOT call any more tools. "
    "Your positions have been closed. Trading will resume tomorrow."
)
_PG_MSG = (
    "🏆 EVALUATION PASSED — your account has reached the profit goal! "
    "Trading is DISABLED. Do NOT call any more tools. Congratulations!"
)


async def _check_eval_limits(agent_name: str) -> Optional[str]:
    """Check if agent has hit Daily Profit Limit or Profit Goal.

    Returns None if trading is allowed, or a stop message string if not.
    Uses TTL-based caching like _check_blown().
    """
    now = _time.monotonic()

    # Check PG cache first (permanent stop)
    cached_at = _pg_agents.get(agent_name)
    if cached_at is not None and (now - cached_at) < _EVAL_CACHE_TTL:
        return _PG_MSG

    # Check DPL cache (daily stop)
    cached_at = _dpl_agents.get(agent_name)
    if cached_at is not None and (now - cached_at) < _EVAL_CACHE_TTL:
        return _DPL_MSG

    if _sim_manager is None:
        return None

    portfolio = await _sim_manager.get_portfolio(agent_name)
    if portfolio.get("error"):
        return None

    from sim_account_manager import get_eval_rules
    starting_balance = portfolio["startingBalance"]
    dpl, pg = get_eval_rules(starting_balance)

    realized_day_pnl = portfolio["realizedDayPnl"]
    balance = portfolio["balance"]
    profit_from_start = balance - starting_balance

    # Check Profit Goal first (balance >= starting + PG)
    if profit_from_start >= pg:
        _pg_agents[agent_name] = now
        logger.info(f"🏆 EVAL PASSED: {agent_name} balance ${balance:,.2f} >= goal ${starting_balance + pg:,.2f}")
        return _PG_MSG

    # Check Daily Profit Limit (realized day P&L >= DPL)
    if realized_day_pnl >= dpl:
        _dpl_agents[agent_name] = now
        logger.info(f"⛔ DPL HIT: {agent_name} realized today ${realized_day_pnl:,.2f} >= limit ${dpl:,.2f}")
        # Auto-close any open positions
        if portfolio.get("positions"):
            try:
                positions = await _sim_manager.get_positions(agent_name)
                for pos in positions:
                    await _sim_manager.execute_close(agent_name, pos["symbol"], 0)
                logger.info(f"Auto-closed all positions for {agent_name} (DPL hit)")
                activity = _get_agent_activity(agent_name)
                activity.append({
                    "ts": datetime.now().isoformat(),
                    "type": "DPL_LIQUIDATION",
                    "msg": f"Daily profit limit ${dpl:,.0f} reached — all positions auto-closed",
                })
            except Exception as e:
                logger.error(f"Error auto-closing positions for {agent_name} on DPL: {e}")
        return _DPL_MSG

    # Clear stale caches if limits not hit (e.g., after daily reset)
    _dpl_agents.pop(agent_name, None)
    _pg_agents.pop(agent_name, None)
    return None


def _ensure_price_streaming(contract_id: str) -> None:
    """Subscribe to price streaming for a contract (for live PnL)."""
    if _price_streamer is not None:
        _price_streamer.subscribe(contract_id)


async def _ensure_live_price(contract_id: str) -> bool:
    """Ensure a live price exists for a contract before trading.

    Subscribes to the RTC stream and, if no price is cached yet,
    does a one-shot REST fetch so the trade can fill immediately.
    Returns True if a price is available.
    """
    _ensure_price_streaming(contract_id)
    if TopstepXAccountClient.get_market_price(contract_id) is not None:
        return True
    # One-shot REST fetch to seed the price for first trade
    if _price_streamer and _price_streamer._account_client:
        price = await _price_streamer._account_client._fetch_current_price(contract_id)
        if price is not None:
            TopstepXAccountClient.update_market_price(contract_id, price)
            return True
    return False


# ── Agent Tools ──────────────────────────────────────────────────


@agent_tool
async def topstepx_buy(
    ctx: ToolContext,
    contract: str,
    quantity: int,
) -> str:
    """Buy futures contracts.

    Args:
        contract: Contract ID from topstepx_available_contracts()
        quantity: Number of contracts to buy (must be positive integer)

    Returns:
        Result message
    """
    if _sim_manager is None:
        return "❌ Simulated trading not initialized"

    from unified_market_connector import _is_market_open
    if not _is_market_open():
        return "❌ Market is CLOSED. No trading allowed until market reopens. Call topstepx_portfolio to check your account."

    if quantity <= 0:
        return "❌ Quantity must be positive"

    agent_name = ctx.agent_name or os.getenv("AGENT_NAME", "default")
    if await _check_blown(agent_name):
        return _BLOWN_MSG
    eval_msg = await _check_eval_limits(agent_name)
    if eval_msg:
        return eval_msg
    logger.info(f"🔵 SIM BUY ORDER: {agent_name} {quantity}x {contract}")
    hedge_err = await _check_live_hedging(agent_name, "BUY")
    if hedge_err:
        logger.error(f"❌ HEDGING BLOCKED: {hedge_err}")
        return f"❌ {hedge_err}"
    await _ensure_live_price(contract)
    result = await _sim_manager.execute_buy(agent_name, contract, quantity)

    if result.get("success"):
        price = result["fill_price"]
        fee = result.get("fee", 0)
        logger.info(f"✅ SIM BUY FILLED: {quantity}x {contract} @ ${price:,.2f} | Fee: ${fee:,.2f}")
        # Mirror to real TopstepX account if this is the promoted agent
        live_line = ""
        if _promoted_agent and agent_name == _promoted_agent:
            live_result = await _mirror_to_live("BUY", contract, quantity, agent_name)
            if live_result.get("success"):
                live_line = f"\n  LIVE: Mirrored to TopstepX account {_live_account_id}"
            else:
                live_line = f"\n  LIVE MIRROR FAILED: {live_result.get('error', 'Unknown')}"
        nt_line = ""
        if _nt_copytrader and _nt_copy_agent and agent_name == _nt_copy_agent:
            nt_result = await _mirror_to_ninjatrader("BUY", contract, quantity, agent_name)
            if nt_result.get("success"):
                nt_line = f"\n  COPY: Sent to NinjaTrader account {_nt_copytrader.account}"
            else:
                nt_line = f"\n  COPY FAILED: {nt_result.get('error', 'Unknown')}"
        return (
            f"✓ BUY order filled\n"
            f"  Contract: {contract}\n"
            f"  Quantity: {quantity}\n"
            f"  Fill Price: ${price:,.2f}\n"
            f"  Commission: ${fee:,.2f}\n"
            f"  Account: {agent_name}"
            + live_line
            + nt_line
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
        contract: Contract ID from topstepx_available_contracts()
        quantity: Number of contracts to sell (must be positive integer)

    Returns:
        Result message
    """
    if _sim_manager is None:
        return "❌ Simulated trading not initialized"

    from unified_market_connector import _is_market_open
    if not _is_market_open():
        return "❌ Market is CLOSED. No trading allowed until market reopens. Call topstepx_portfolio to check your account."

    if quantity <= 0:
        return "❌ Quantity must be positive"

    agent_name = ctx.agent_name or os.getenv("AGENT_NAME", "default")
    if await _check_blown(agent_name):
        return _BLOWN_MSG
    eval_msg = await _check_eval_limits(agent_name)
    if eval_msg:
        return eval_msg
    logger.info(f"🔴 SIM SELL ORDER: {agent_name} {quantity}x {contract}")
    hedge_err = await _check_live_hedging(agent_name, "SELL")
    if hedge_err:
        logger.error(f"❌ HEDGING BLOCKED: {hedge_err}")
        return f"❌ {hedge_err}"
    await _ensure_live_price(contract)
    result = await _sim_manager.execute_sell(agent_name, contract, quantity)

    if result.get("success"):
        price = result["fill_price"]
        fee = result.get("fee", 0)
        logger.info(f"✅ SIM SELL FILLED: {quantity}x {contract} @ ${price:,.2f} | Fee: ${fee:,.2f}")
        live_line = ""
        if _promoted_agent and agent_name == _promoted_agent:
            live_result = await _mirror_to_live("SELL", contract, quantity, agent_name)
            if live_result.get("success"):
                live_line = f"\n  LIVE: Mirrored to TopstepX account {_live_account_id}"
            else:
                live_line = f"\n  LIVE MIRROR FAILED: {live_result.get('error', 'Unknown')}"
        nt_line = ""
        if _nt_copytrader and _nt_copy_agent and agent_name == _nt_copy_agent:
            nt_result = await _mirror_to_ninjatrader("SELL", contract, quantity, agent_name)
            if nt_result.get("success"):
                nt_line = f"\n  COPY: Sent to NinjaTrader account {_nt_copytrader.account}"
            else:
                nt_line = f"\n  COPY FAILED: {nt_result.get('error', 'Unknown')}"
        return (
            f"✓ SELL order filled\n"
            f"  Contract: {contract}\n"
            f"  Quantity: {quantity}\n"
            f"  Fill Price: ${price:,.2f}\n"
            f"  Commission: ${fee:,.2f}\n"
            f"  Account: {agent_name}"
            + live_line
            + nt_line
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
        contract: Contract ID from topstepx_available_contracts()
        quantity: Number of contracts to close. 0 = close all.

    Returns:
        Result message
    """
    if _sim_manager is None:
        return "❌ Simulated trading not initialized"

    agent_name = ctx.agent_name or os.getenv("AGENT_NAME", "default")
    if await _check_blown(agent_name):
        return _BLOWN_MSG
    # Allow closes when DPL is hit (positions are auto-closed, but don't block manual close)
    # Block new trades (buy/sell) but not closing existing positions
    pg_msg = _pg_agents.get(agent_name)
    if pg_msg is not None and (_time.monotonic() - pg_msg) < _EVAL_CACHE_TTL:
        return _PG_MSG
    logger.info(f"🔶 SIM CLOSE: {agent_name} {contract} qty={quantity}")
    result = await _sim_manager.execute_close(agent_name, contract, quantity)

    if result.get("success"):
        price = result["fill_price"]
        pnl = result["realized_pnl"]
        qty_closed = result["quantity_closed"]
        logger.info(f"✅ SIM CLOSE: {qty_closed}x {contract} @ ${price:,.2f} | PnL: ${pnl:+,.2f}")
        # Mirror close to real TopstepX account
        live_line = ""
        if _promoted_agent and agent_name == _promoted_agent:
            live_result = await _mirror_to_live("CLOSE", contract, quantity, agent_name)
            if live_result.get("success"):
                live_line = f"  LIVE: Mirrored close to TopstepX account {_live_account_id}"
            else:
                live_line = f"  LIVE MIRROR FAILED: {live_result.get('error', 'Unknown')}"
        nt_line = ""
        if _nt_copytrader and _nt_copy_agent and agent_name == _nt_copy_agent:
            nt_result = await _mirror_to_ninjatrader("CLOSE", contract, quantity, agent_name)
            if nt_result.get("success"):
                nt_line = f"  COPY: Closed on NinjaTrader account {_nt_copytrader.account}"
            else:
                nt_line = f"  COPY FAILED: {nt_result.get('error', 'Unknown')}"
        fee = result.get("fee", 0)
        lines = [
            f"✓ Position CLOSED",
            f"  Contract: {contract}",
            f"  Quantity closed: {qty_closed}",
            f"  Fill Price: ${price:,.2f}",
            f"  Realized P&L: ${pnl:+,.2f}",
            f"  Commission: ${fee:,.2f}",
            f"  New Balance: ${result['new_balance']:,.2f}",
        ]
        if live_line:
            lines.append(live_line)
        if nt_line:
            lines.append(nt_line)
        if result.get("blown"):
            lines.append(f"  ⚠️ {result['warning']}")
        # Post-close eval limit check: if this close pushed day P&L over DPL, notify + auto-close remaining
        post_eval = await _check_eval_limits(agent_name)
        if post_eval:
            lines.append(f"\n{post_eval}")
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
    if await _check_blown(agent_name):
        return _BLOWN_MSG
    eval_msg = await _check_eval_limits(agent_name)
    if eval_msg:
        return eval_msg

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
async def topstepx_available_contracts(ctx: ToolContext) -> str:
    """Get list of available futures contracts you can trade.

    Returns:
        Table of tradeable contracts with specs and commission info
    """
    if _sim_manager is None:
        return "❌ Simulated trading not initialized"

    agent_name = ctx.agent_name or os.getenv("AGENT_NAME", "default")
    if await _check_blown(agent_name):
        return _BLOWN_MSG

    contracts = await _sim_manager.get_active_contracts()
    if not contracts:
        return "No contracts available. Contract data may not be synced yet."

    lines = [
        "AVAILABLE CONTRACTS:",
        f"{'Contract ID':<28} {'Group':>5} {'Type':>6} {'Tick':>6} {'$/Tick':>7} {'RT Fee':>7} {'MicroEq':>7}",
        "-" * 78,
    ]
    for c in contracts:
        ctype = "MICRO" if c["is_micro"] else "FULL"
        lines.append(
            f"{c['contract_id']:<28} {c['contract_group']:>5} {ctype:>6} "
            f"{c['tick_size']:>6.4g} ${c['tick_value']:>5.2f} ${c['commission_rt']:>5.2f} "
            f"{c['micro_equivalent']:>7d}"
        )
    lines.append(f"\nPosition limit: 1 full-size = 10 micro-equivalents. 150K account = 150 micro-equiv max.")
    lines.append("Use topstepx_retrieve_bars(contract_id, timeframe, bars) to analyze price history before trading.")
    return "\n".join(lines)


# ── Bars cache for retrieve_bars ──
_bars_cache: dict[str, tuple[float, str]] = {}  # key -> (timestamp, result)
_BARS_CACHE_TTL = 45.0  # seconds — shared across all agents
_bars_rate_limit_until: float = 0.0  # backoff timestamp after 429
_bars_lock = asyncio.Lock()  # serialize concurrent retrieve_bars calls
# Rolling window rate limiter: track timestamps of recent API calls
_bars_call_timestamps: list[float] = []
_BARS_RATE_LIMIT = 40  # stay under 50/30s with headroom for price polling
_BARS_RATE_WINDOW = 30.0


def _detect_fvgs(bars: list[dict], current_price: float) -> list[str]:
    """Detect Fair Value Gaps and Inverse FVGs from OHLCV bars.

    FVG (Fair Value Gap): 3-candle imbalance where candle 1 and candle 3
    don't overlap, leaving a gap at candle 2.
      - Bullish FVG: bar[i-1].high < bar[i+1].low  (gap up)
      - Bearish FVG: bar[i-1].low > bar[i+1].high   (gap down)

    Status tracking using subsequent bars:
      - untested: price hasn't returned to the gap zone
      - tested: price wicked into the gap but closed outside (respected)
      - filled: price closed inside or through the gap (no longer active)
      - IFVG (invalidated): a previously tested/respected FVG that price
        later closed through — creates a new zone in the invalidation direction

    Returns list of formatted lines, or empty list if no gaps found.
    """
    if len(bars) < 3:
        return []

    fvgs: list[dict] = []

    for i in range(1, len(bars) - 1):
        h_prev = float(bars[i - 1]["h"])
        l_prev = float(bars[i - 1]["l"])
        h_next = float(bars[i + 1]["h"])
        l_next = float(bars[i + 1]["l"])

        if h_prev < l_next:
            # Bullish FVG: gap between bar[i-1] high and bar[i+1] low
            fvgs.append({
                "type": "BULLISH",
                "top": l_next,
                "bottom": h_prev,
                "time": bars[i]["t"],
                "bar_idx": i,
                "tested": False,
                "respected": False,
                "filled": False,
            })
        elif l_prev > h_next:
            # Bearish FVG: gap between bar[i+1] high and bar[i-1] low
            fvgs.append({
                "type": "BEARISH",
                "top": l_prev,
                "bottom": h_next,
                "time": bars[i]["t"],
                "bar_idx": i,
                "tested": False,
                "respected": False,
                "filled": False,
            })

    if not fvgs:
        return ["Fair Value Gaps: none detected"]

    # Track status using bars after each FVG formed
    # States: untested -> tested -> respected (bounced) or filled/invalidated
    ifvgs: list[dict] = []
    for fvg in fvgs:
        for j in range(fvg["bar_idx"] + 2, len(bars)):
            bar_low = float(bars[j]["l"])
            bar_high = float(bars[j]["h"])
            bar_close = float(bars[j]["c"])

            if fvg["type"] == "BULLISH":
                # Price wicked into or through the gap zone
                if bar_low <= fvg["top"]:
                    fvg["tested"] = True
                # After testing, price closed back above the gap = respected
                if fvg["tested"] and bar_close > fvg["top"]:
                    fvg["respected"] = True
                # Price closed below the gap bottom = filled/invalidated
                if bar_close < fvg["bottom"]:
                    fvg["filled"] = True
                    if fvg["tested"]:
                        ifvgs.append({
                            "type": "BEARISH IFVG",
                            "top": fvg["top"],
                            "bottom": fvg["bottom"],
                            "time": bars[j]["t"],
                            "origin_time": fvg["time"],
                        })
                    break
            else:  # BEARISH
                if bar_high >= fvg["bottom"]:
                    fvg["tested"] = True
                if fvg["tested"] and bar_close < fvg["bottom"]:
                    fvg["respected"] = True
                if bar_close > fvg["top"]:
                    fvg["filled"] = True
                    if fvg["tested"]:
                        ifvgs.append({
                            "type": "BULLISH IFVG",
                            "top": fvg["top"],
                            "bottom": fvg["bottom"],
                            "time": bars[j]["t"],
                            "origin_time": fvg["time"],
                        })
                    break

    # Build output — only show active (unfilled) FVGs and recent IFVGs
    active = [f for f in fvgs if not f["filled"]]
    lines = ["Fair Value Gaps:"]

    if not active and not ifvgs:
        lines.append("  No active FVGs (all filled)")
        return lines

    for fvg in active:
        status = "respected" if fvg["respected"] else "tested" if fvg["tested"] else "untested"
        mid = (fvg["top"] + fvg["bottom"]) / 2
        dist = current_price - mid
        direction = "above" if dist > 0 else "below"
        lines.append(
            f"  {fvg['type']} FVG @ {fvg['bottom']:.2f}-{fvg['top']:.2f} "
            f"(formed {fvg['time']}, {status}, "
            f"price {abs(dist):.2f} {direction} midpoint)"
        )

    for ifvg in ifvgs:
        mid = (ifvg["top"] + ifvg["bottom"]) / 2
        dist = current_price - mid
        direction = "above" if dist > 0 else "below"
        lines.append(
            f"  {ifvg['type']} @ {ifvg['bottom']:.2f}-{ifvg['top']:.2f} "
            f"(FVG formed {ifvg['origin_time']}, invalidated {ifvg['time']}, "
            f"price {abs(dist):.2f} {direction} midpoint)"
        )

    return lines


@agent_tool
async def topstepx_retrieve_bars(
    ctx: ToolContext,
    contract_id: str,
    timeframe: str = "5min",
    bars: int = 20,
) -> str:
    """Retrieve historical price bars for any available contract.

    Args:
        contract_id: Contract ID from topstepx_available_contracts()
        timeframe: Bar timeframe: "1min", "5min", "15min", "1hour", "4hour"
        bars: Number of bars to retrieve (max 50)

    Returns:
        OHLCV price data for the requested contract
    """
    if _sim_manager is None:
        return "❌ Simulated trading not initialized"

    agent_name = ctx.agent_name or os.getenv("AGENT_NAME", "default")
    if await _check_blown(agent_name):
        return _BLOWN_MSG

    # Validate contract exists
    info = await _sim_manager._get_contract_info(contract_id)
    if info is None:
        # Suggest similar contracts from the cache
        parts = contract_id.split(".")
        base = parts[-2] if len(parts) >= 2 else ""
        suggestions = [cid for cid in _sim_manager._contract_cache if base and base in cid]
        hint = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
        return f"❌ Unknown contract: {contract_id}. Call topstepx_available_contracts() to see valid contracts.{hint}"

    # Clamp bars
    bars = max(1, min(bars, 50))

    # Map timeframe string to seconds
    tf_map = {
        "1min": 60, "5min": 300, "15min": 900,
        "1hour": 3600, "4hour": 14400,
    }
    granularity = tf_map.get(timeframe)
    if granularity is None:
        return f"❌ Invalid timeframe '{timeframe}'. Use: {', '.join(tf_map.keys())}"

    cache_key = f"{contract_id}:{timeframe}"

    # Serialize concurrent calls so they share cache instead of all hitting API
    async with _bars_lock:
        now = _time.time()
        cached = _bars_cache.get(cache_key)
        if cached and (now - cached[0]) < _BARS_CACHE_TTL:
            return cached[1]

        # Global rate limit backoff after 429
        global _bars_rate_limit_until
        if now < _bars_rate_limit_until:
            if cached:
                return cached[1]
            wait = int(_bars_rate_limit_until - now)
            return f"Rate limited — retry in {wait}s. Call topstepx_portfolio to check your positions instead."

        # Rolling window rate limit: stay under 40 calls per 30s
        cutoff = now - _BARS_RATE_WINDOW
        _bars_call_timestamps[:] = [t for t in _bars_call_timestamps if t > cutoff]
        if len(_bars_call_timestamps) >= _BARS_RATE_LIMIT:
            if cached:
                return cached[1]
            return "Rate limit reached — too many bar requests. Call topstepx_portfolio to check your positions instead."

        jwt_token = os.getenv("TOPSTEPX_JWT_TOKEN", "")
        if not jwt_token:
            return "❌ No API token available for fetching bars"

        import httpx
        from datetime import timedelta, timezone as tz

        now_utc = datetime.now(tz.utc)
        fetch_bars = 50
        lookback_seconds = granularity * fetch_bars * 2
        start_time = now_utc - timedelta(seconds=lookback_seconds)

        if granularity < 60:
            unit, unit_number = 1, granularity
        elif granularity < 3600:
            unit, unit_number = 2, granularity // 60
        else:
            unit, unit_number = 3, granularity // 3600

        try:
            api_base = os.getenv("TOPSTEPX_API_URL", "https://api.topstepx.com")
            async with httpx.AsyncClient(
                timeout=15.0,
                headers={"Authorization": f"Bearer {jwt_token}"},
            ) as client:
                _bars_call_timestamps.append(_time.time())
                resp = await client.post(
                    f"{api_base}/api/History/retrieveBars",
                    json={
                        "contractId": contract_id,
                        "live": False,
                        "startTime": start_time.isoformat(),
                        "endTime": now_utc.isoformat(),
                        "unit": unit,
                        "unitNumber": unit_number,
                        "limit": fetch_bars,
                        "includePartialBar": True,
                    },
                )
                if resp.status_code == 429:
                    _bars_rate_limit_until = _time.time() + 30.0
                    logger.warning("retrieve_bars 429 rate limited — backing off 30s")
                    if cached:
                        return cached[1]
                    return "Rate limited by TopstepX API. Call topstepx_portfolio to check your positions instead."
                resp.raise_for_status()
                data = resp.json()

            if not data.get("success"):
                return f"❌ API error: {data.get('errorMessage', 'Unknown')}"

            all_bars = data.get("bars", [])
            if not all_bars:
                return f"No bars returned for {contract_id} ({timeframe}). Market may be closed."

            latest_close = float(all_bars[-1]["c"])
            if TopstepXAccountClient.get_contract_specs(contract_id) is None:
                TopstepXAccountClient.update_contract_specs(
                    contract_id, info["tick_size"], info["tick_value"]
                )

            display_bars = all_bars[-bars:]
            desc = info.get("description") or info.get("name") or contract_id
            lines = [
                f"[{desc}] {timeframe} bars (latest {len(display_bars)}):",
                "Time,Open,High,Low,Close,Volume",
            ]
            for bar in display_bars:
                lines.append(
                    f"{bar['t']},{bar['o']},{bar['h']},{bar['l']},{bar['c']},{bar['v']}"
                )
            lines.append(f"\nCurrent price: ${latest_close:,.2f}")

            # ── FVG / IFVG detection ───────────────────────────────
            fvg_lines = _detect_fvgs(all_bars, latest_close)
            if fvg_lines:
                lines.append("")
                lines.extend(fvg_lines)

            result = "\n".join(lines)
            _bars_cache[cache_key] = (_time.time(), result)
            return result

        except Exception as e:
            logger.error(f"retrieve_bars error for {contract_id}: {e}")
            return f"❌ Failed to fetch bars: {e}"


@agent_tool
async def topstepx_portfolio(ctx: ToolContext) -> str:
    """Get portfolio status with real-time P&L.

    Returns:
        Portfolio summary with live position data
    """
    if _sim_manager is None:
        return "❌ Simulated trading not initialized"

    agent_name = ctx.agent_name or os.getenv("AGENT_NAME", "default")
    reset_notice = _consume_reset_notice(agent_name)
    if await _check_blown(agent_name):
        return _BLOWN_MSG

    # Per-agent cache: when flat, return cached result for 30s to avoid spamming DB
    # Skip cache if there's a reset notice to deliver
    import time as _time
    now = _time.time()
    cache = _portfolio_caches.setdefault(agent_name, {"result": None, "time": 0.0, "has_positions": False})
    if (
        not reset_notice
        and cache["result"] is not None
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

    # For promoted agent: use real TopstepX balance and merge live positions
    # (e.g., positions opened directly on the TopstepX platform)
    if _promoted_agent and agent_name == _promoted_agent and _trading_client and _live_account_id:
        try:
            live_summary = await _trading_client.get_account_summary(_live_account_id)
            if "error" not in live_summary:
                balance = live_summary["balance"]
                logger.info(f"💼 Using live TopstepX balance: ${balance:,.2f} (sim was ${summary['balance']:,.2f})")

                live_positions = live_summary.get("positions", [])
                sim_symbols = {p["symbol"] for p in positions}
                for lp in live_positions:
                    if lp["symbol"] not in sim_symbols and lp["quantity"] != 0:
                        lp["_live_only"] = True
                        positions.append(lp)
                        _ensure_price_streaming(lp["symbol"])
                        logger.info(f"💼 LIVE POSITION (not in sim): {lp['symbol']} qty={lp['quantity']} avg=${lp['avgPrice']:,.2f}")
        except Exception as e:
            logger.warning(f"Failed to fetch live account data for portfolio: {e}")

    if not positions:
        logger.info(f"💼 PORTFOLIO: No open positions | Balance: ${balance:,.2f}")
        state = _get_agent_state(agent_name)
        state["last_active"] = datetime.now().isoformat()
        result = (
            f"YOU HOLD: nothing — 0 open positions.\n"
            f"Balance: ${balance:,.2f} | Equity: ${balance:,.2f}\n"
            f"---\nNOW call report_sentiment(reasoning=<your analysis>, sentiment=<bullish|bearish|neutral>)"
        )
        result += reset_notice
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

        live_tag = " [LIVE]" if pos.get("_live_only") else ""
        hold_parts.append(f"{direction} {abs_qty}x {pos['symbol']}{live_tag} (P&L: ${pnl:+,.2f})")
        logger.info(f"💼 POSITION: {pos['symbol']}{live_tag} {direction} {abs_qty} @ ${avg:,.2f} | P&L: ${pnl:+,.2f}")

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
    result = "\n".join(lines) + reset_notice
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
    all_agents = set()
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
    # Only use AGENT_NAME fallback if no agents discovered from config
    if not all_agents:
        all_agents.add(agent_name)
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
            _sim_manager.register_agent_config(name, state["model"], state["strategy"])
            logger.info(f"Pre-populated dashboard panel for agent: {name} (model={state['model']}, strategy={state['strategy']})")

    # Start background MLL monitor + daily reset scheduler
    bg_tasks = await _sim_manager.start_background_tasks()

    # Start market-close liquidation monitor
    async def _market_close_monitor():
        """Auto-liquidate all positions when market closes."""
        from unified_market_connector import _is_market_open
        was_open = _is_market_open()
        # On startup, if market is already closed, liquidate any stale positions
        if not was_open:
            try:
                results = await _sim_manager.liquidate_all_positions()
                if results:
                    logger.info(f"Startup liquidation (market closed): {len(results)} position(s) closed")
            except Exception as e:
                logger.error(f"Startup liquidation error: {e}")
        while True:
            try:
                is_open = _is_market_open()
                # Detect market close transition (open -> closed)
                if was_open and not is_open:
                    logger.info("MARKET CLOSED — liquidating all open positions")
                    results = await _sim_manager.liquidate_all_positions()
                    for r in results:
                        agent = r.get("agent_name", "?")
                        if r.get("success"):
                            # Record liquidation in agent activity feed
                            activity = _get_agent_activity(agent)
                            activity.append({
                                "ts": datetime.now().isoformat(),
                                "type": "LIQUIDATION",
                                "msg": f"Market close: {r['symbol']} closed @ ${r['fill_price']:,.2f} (P&L: ${r['realized_pnl']:+,.2f})",
                            })
                    if results:
                        logger.info(f"Market-close liquidation complete: {len(results)} position(s) closed")
                    else:
                        logger.info("Market closed — no open positions to liquidate")
                was_open = is_open
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"Market-close monitor error: {e}")
            await asyncio.sleep(30)

    asyncio.create_task(_market_close_monitor())
    logger.info("Market-close liquidation monitor started")

    # ── Authenticate with TopstepX (always fetch fresh JWT on startup) ──
    from topstepx_auth import TopstepXTokenManager, set_token_manager

    username = os.getenv("TOPSTEPX_USERNAME")
    api_key = os.getenv("TOPSTEPX_API_KEY")
    _token_manager = None
    if username and api_key:
        _token_manager = TopstepXTokenManager(
            username, api_key,
            os.getenv("TOPSTEPX_ENVIRONMENT", "demo"),
            os.getenv("TOPSTEPX_API_URL"),
        )
        logger.info("Authenticating with TopstepX (fresh JWT)...")
        jwt_token = await _token_manager.start()
        if not jwt_token:
            logger.warning("TopstepX authentication failed — RTC prices + trade mirroring disabled")
        else:
            logger.info("Authenticated successfully — JWT token obtained (auto-refresh enabled)")
        set_token_manager(_token_manager)
    elif os.getenv("TOPSTEPX_JWT_TOKEN"):
        logger.info("Using existing TOPSTEPX_JWT_TOKEN (no auto-refresh — set USERNAME + API_KEY for auto-refresh)")
    else:
        logger.info("TopstepX credentials not set — RTC prices + trade mirroring disabled")

    # Sync contracts from TopstepX API into database
    jwt_for_sync = os.getenv("TOPSTEPX_JWT_TOKEN", "")
    if jwt_for_sync:
        api_url = os.getenv("TOPSTEPX_API_URL", "https://api.topstepx.com")
        synced = await _sim_manager.sync_contracts(jwt_for_sync, api_url)
        logger.info(f"Contract sync: {synced} contracts loaded into database")
    else:
        logger.warning("No JWT token — skipping contract sync (using fallback specs)")

    # Initialize TopstepX client for RTC prices, account stats, and live trading
    _init_client()
    _web_client = None
    _dashboard_client = None
    global _promoted_agent
    if _trading_client:
        # Register trading client's HTTP client for token refresh
        if _token_manager:
            _token_manager.register_http_client(_trading_client._http_client)

        await _ensure_live_account()
        if _live_account_id:
            # PROMOTED_AGENT takes precedence over legacy MIRROR_AGENT
            _promoted_agent = os.getenv("PROMOTED_AGENT", "") or os.getenv("MIRROR_AGENT", "")
            if _promoted_agent:
                logger.info(f"LIVE TRADING enabled: {_promoted_agent} -> TopstepX account {_live_account_id}")
            else:
                logger.info(f"TopstepX account {_live_account_id} found (set PROMOTED_AGENT to enable live trading)")
        else:
            logger.warning("No TopstepX account found — live trading disabled")

        # Initialize web clients for practice account stats
        jwt_token_val = os.getenv("TOPSTEPX_JWT_TOKEN", "")
        if jwt_token_val:
            from topstepx_web_client import TopstepDashboardClient, TopstepXWebClient
            _web_client = TopstepXWebClient(jwt_token_val)
            logger.info("TopstepX web client initialized")
            # Register web client's HTTP client for token refresh
            if _token_manager:
                _token_manager.register_http_client(_web_client._http)

            dash_refresh = os.getenv("TOPSTEP_REFRESH_TOKEN", "").strip()
            if dash_refresh:
                _dashboard_client = TopstepDashboardClient(refresh_token=dash_refresh)
                logger.info("Topstep dashboard client initialized")
            else:
                logger.info("TOPSTEP_REFRESH_TOKEN not set — dashboard balance history disabled")

    # Start real-time price streaming (subscribes dynamically when agents enter positions)
    global _price_streamer
    price_streamer = None
    jwt_token = os.getenv("TOPSTEPX_JWT_TOKEN", "")
    if jwt_token:
        ws_base = os.getenv("TOPSTEPX_API_URL", "https://api.topstepx.com").replace("api.", "rtc.")
        price_streamer = TopstepXPriceStreamer(
            jwt_token, [], ws_base=ws_base,
            account_client=_trading_client._account_client if _trading_client else None,
            account_id=_live_account_id,
        )
        # Register streamer for token refresh (WS URLs use token)
        if _token_manager:
            _token_manager.register_ws_token_setter(price_streamer.set_token)
        await price_streamer.start()
        _price_streamer = price_streamer
        logger.info("RTC price streamer started (dynamic subscription on position entry)")

        # Subscribe to contracts with existing open positions (sim + live)
        if _sim_manager:
            try:
                open_symbols = await _sim_manager.get_all_open_position_symbols()
                for sym in open_symbols:
                    price_streamer.subscribe(sym)
                if open_symbols:
                    logger.info(f"Subscribed to existing sim position contracts: {open_symbols}")
            except Exception as e:
                logger.warning(f"Failed to subscribe to existing sim positions: {e}")

        # Also subscribe to any live TopstepX positions (opened on platform directly)
        if _trading_client and _live_account_id:
            try:
                live_positions = await _trading_client._account_client.get_positions(_live_account_id)
                for pos in live_positions:
                    if pos.quantity != 0:
                        price_streamer.subscribe(pos.symbol)
                        logger.info(f"Subscribed to live position contract: {pos.symbol}")
            except Exception as e:
                logger.warning(f"Failed to subscribe to live positions: {e}")

    # ── Initialize NinjaTrader copy-trader (independent of TopstepX) ──
    global _nt_copytrader, _nt_copy_agent
    nt_enabled = os.getenv("NINJATRADER_ENABLED", "true").strip().lower() not in ("0", "false", "no", "")
    if nt_enabled:
        nt_url = os.getenv("NINJATRADER_BRIDGE_URL", "http://localhost:5000")
        nt_account = os.getenv("NINJATRADER_ACCOUNT", "TOF130830").strip()
        # Copy agent defaults to the promoted agent so the same agent that mirrors
        # to TopstepX also copy-trades to NinjaTrader.
        _nt_copy_agent = os.getenv("NINJATRADER_COPY_AGENT", "").strip() or _promoted_agent
        if not _nt_copy_agent:
            logger.info("NinjaTrader copy trading: no copy agent set (PROMOTED_AGENT/NINJATRADER_COPY_AGENT) — disabled")
        else:
            try:
                from ninjatrader_bridge import create_copytrader
                _nt_copytrader = await create_copytrader(nt_url, nt_account)
                if _nt_copytrader:
                    logger.info(f"COPY TRADING enabled: {_nt_copy_agent} -> NinjaTrader account {nt_account}")
                else:
                    logger.warning("NinjaTrader copy trading disabled (bridge/account unavailable)")
            except Exception as e:
                logger.error(f"Failed to initialize NinjaTrader copy-trader: {e}")
                _nt_copytrader = None
    else:
        logger.info("NinjaTrader copy trading disabled (NINJATRADER_ENABLED=false)")

    print("=" * 60)
    print("TopstepX Trading Arena (Simulated Accounts + RTC Prices)")
    print("=" * 60)

    # Initialize Kafka
    print(f"\nConnecting to Kafka at {args.bootstrap_servers}...")
    broker = BrokerClient(bootstrap_servers=args.bootstrap_servers)
    service = NodesService(broker)

    # Subscribe to agent output for dashboard activity panel
    @broker.subscriber("agent_router.output", group_id="dashboard-agent-viewer")
    async def handle_agent_output(envelope: EventEnvelope) -> None:
        _process_agent_envelope(envelope)

    # Register tools
    print("\nRegistering trading tools:")
    tools = [topstepx_buy, topstepx_sell, topstepx_close, topstepx_portfolio, topstepx_available_contracts, topstepx_retrieve_bars, report_sentiment]
    for tool in tools:
        service.register_node(tool)
        print(f"  ✓ {tool.tool_schema.name} - {tool.tool_schema.description}")

    print(f"\n✓ Simulated trading enabled for agent: {agent_name}")
    if _live_account_id:
        print(f"✓ RTC prices streaming | TopstepX account: {_live_account_id}")
    if _promoted_agent:
        print(f"✓ LIVE TRADING: {_promoted_agent} -> TopstepX account {_live_account_id}")
    if _nt_copytrader:
        print(f"✓ COPY TRADING: {_nt_copy_agent} -> NinjaTrader account {_nt_copytrader.account}")
    print("\nPress Ctrl+C to stop...")

    # ── Start web dashboard alongside Kafka service ────────────
    dashboard_port = int(os.getenv("DASHBOARD_PORT", "8080"))
    try:
        import socket

        import uvicorn
        from topstepx_web_dashboard import create_app

        def _set_account_id(new_id: int):
            global _live_account_id
            if new_id != _live_account_id:
                logger.info(f"Account ID refreshed: {_live_account_id} -> {new_id}")
                _live_account_id = new_id

        dashboard_app = create_app(
            sim_manager=_sim_manager,
            get_all_agents_state=get_all_agents_state,
            agent_name=agent_name,
            promoted_agent=_promoted_agent,
            trading_client=_trading_client,
            get_account_id=lambda: _live_account_id,
            set_account_id=_set_account_id,
            web_client=_web_client,
            dashboard_client=_dashboard_client,
            ninjatrader_copytrader=_nt_copytrader,
            copy_agent=_nt_copy_agent,
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
        if _nt_copytrader:
            await _nt_copytrader.close()
        if _token_manager:
            await _token_manager.close()
        if _sim_manager:
            await _sim_manager.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTopstepX trading tools stopped.")
