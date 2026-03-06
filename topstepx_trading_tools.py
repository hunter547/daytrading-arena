"""
TopstepX Trading Tools for Agents

Provides @agent_tool functions that allow AI agents to execute real trades
on TopstepX practice accounts. Includes buy, sell, and portfolio query tools.

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


# ── TopstepX Trading Client ──────────────────────────────────────

class TopstepXTradingClient:
    """Client for executing trades on TopstepX."""
    
    def __init__(self, jwt_token: str, api_base_url: str = "https://api.topstepx.com"):
        """Initialize trading client.
        
        Args:
            jwt_token: JWT authentication token
            api_base_url: Base URL for TopstepX API
        """
        self._account_client = TopstepXAccountClient(jwt_token, api_base_url)
        self._api_base = api_base_url.rstrip('/')
        self._http_client = self._account_client._http_client
    
    async def get_practice_account_id(self) -> Optional[int]:
        """Get the practice account ID.
        
        Returns:
            Account ID of the practice account, or None if not found
        """
        accounts = await self._account_client.get_accounts()
        for account in accounts:
            if "PRAC" in account.name:
                return int(account.account_id)
        return None
    
    async def place_market_order(
        self,
        account_id: int,
        contract_id: str,
        side: int,  # OrderSide.BUY or OrderSide.SELL
        size: int,
    ) -> dict:
        """Place a market order.
        
        Args:
            account_id: Account ID to trade in
            contract_id: Contract ID (e.g., "CON.F.US.MES.H26")
            side: Order side (0=Buy, 1=Sell)
            size: Number of contracts
            
        Returns:
            Order response dictionary
        """
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
                return {
                    "success": False,
                    "error": error_msg,
                    "errorCode": error_code,
                }
            
            logger.info(f"Order placed successfully: {data}")
            return data
            
        except Exception as e:
            logger.error(f"Error placing order: {e}")
            return {
                "success": False,
                "error": str(e),
            }
    
    async def close_position(self, account_id: int, contract_id: str) -> dict:
        """Close an entire position for a contract.

        Uses POST /api/Position/closeContract.
        """
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
        """Partially close a position for a contract.

        Uses POST /api/Position/partialCloseContract.
        """
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
        """Get account summary with positions.

        Args:
            account_id: Account ID to query

        Returns:
            Account summary dictionary
        """
        accounts = await self._account_client.get_accounts()
        # Try the requested account first, then fall back to any practice account
        # (handles account reset where ID changes)
        target = None
        for account in accounts:
            if str(account.account_id) == str(account_id):
                target = account
                break
        if target is None:
            # Account ID is stale (e.g. practice account was reset)
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
        """Close HTTP client."""
        await self._account_client.close()


# ── Module-level client ──────────────────────────────────────────

_trading_client: Optional[TopstepXTradingClient] = None
_practice_account_id: Optional[int] = None


def _init_client():
    """Initialize trading client if not already initialized."""
    global _trading_client, _practice_account_id
    
    if _trading_client is not None:
        return
    
    jwt_token = os.getenv("TOPSTEPX_JWT_TOKEN")
    if not jwt_token:
        logger.warning("TOPSTEPX_JWT_TOKEN not set - TopstepX trading disabled")
        return
    
    api_url = os.getenv("TOPSTEPX_API_URL", "https://api.topstepx.com")
    _trading_client = TopstepXTradingClient(jwt_token, api_url)
    logger.info("TopstepX trading client initialized")


async def _ensure_practice_account(force_refresh: bool = False):
    """Ensure practice account ID is loaded (re-fetches if stale or forced)."""
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


# ── Agent activity state ─────────────────────────────────────────

BULLISH_KEYWORDS = {"bullish", "upward", "scaling in", "buy", "long", "momentum"}
BEARISH_KEYWORDS = {"bearish", "downward", "cutting", "sell", "short", "risk off"}
NEUTRAL_KEYWORDS = {"flat", "wait", "no clear", "patience"}

_agent_state: dict = {
    "agent_name": os.getenv("AGENT_NAME", ""),
    "model": os.getenv("AGENT_MODEL", ""),
    "strategy": os.getenv("AGENT_STRATEGY", ""),
    "sentiment": "neutral",
    "last_active": None,
    "latest_reasoning": None,
    "activity": [],  # last ~10 events
}
_agent_activity: deque = deque(maxlen=10)
_agent_seen: set = set()


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
    r'\b(long|short|flat|holding|hold|held|positions?|contracts?|MES|MNQ|unrealized|P&L|pnl|no open|no entries)\b',
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
    """Parse an EventEnvelope and update _agent_state."""
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

    now = datetime.now().strftime("%H:%M:%S")
    _agent_state["agent_name"] = agent_name
    _agent_state["last_active"] = datetime.now().isoformat()

    for msg in messages_to_check:
        if isinstance(msg, ModelResponse):
            tool_calls = [p for p in msg.parts if isinstance(p, ToolCallPart)]
            text_parts = [p.content for p in msg.parts if isinstance(p, TextPart)]

            if tool_calls:
                lines = [_format_tool_call(tc) for tc in tool_calls]
                _agent_activity.append({"time": now, "kind": "TOOL CALL", "details": "\n".join(lines)})
            if text_parts:
                text = " ".join(text_parts)
                # Do NOT update latest_reasoning from raw LLM text — it hallucinates
                # positions. Dashboard reasoning is only set by report_sentiment and
                # portfolio tool returns (ground-truth paths).
                sentiment = _extract_sentiment(text)
                _agent_state["sentiment"] = sentiment
                logger.info(f"Sentiment extracted from text: {sentiment} | Reasoning: {_truncate(text, 150)}")
                if not tool_calls:
                    _agent_activity.append({"time": now, "kind": "RESPONSE", "details": _truncate(text, 300)})
            elif tool_calls:
                # Tier 3: no text at all — infer sentiment from tool calls
                sentiment = _infer_sentiment_from_tool_calls(tool_calls)
                _agent_state["sentiment"] = sentiment
                tool_names = ", ".join(tc.tool_name for tc in tool_calls)
                logger.info(f"Sentiment inferred from tool calls: {sentiment} | Tools: {tool_names}")

        elif isinstance(msg, ModelRequest):
            tool_returns = [p for p in msg.parts if isinstance(p, ToolReturnPart)]
            if tool_returns:
                lines = [
                    f"{tr.tool_name} -> {_truncate(tr.model_response_str(), 200)}"
                    for tr in tool_returns
                ]
                _agent_activity.append({"time": now, "kind": "TOOL RESULT", "details": "\n".join(lines)})
                # Use portfolio tool results to keep dashboard positions accurate
                for tr in tool_returns:
                    if tr.tool_name == "topstepx_portfolio":
                        result = tr.model_response_str()
                        # Extract the "YOU HOLD: ..." line as ground-truth
                        for line in result.split("\n"):
                            if line.startswith("YOU HOLD:"):
                                portfolio_info = line.strip()
                                existing = _agent_state.get("latest_reasoning") or ""
                                # Replace any old [LONG/SHORT/Flat] prefix
                                existing = re.sub(r"^\[.*?\]\s*", "", existing)
                                existing = _filter_position_hallucinations(existing)
                                _agent_state["latest_reasoning"] = f"[{portfolio_info}] {existing}"
                                break

    _agent_state["activity"] = list(_agent_activity)


def get_agent_state() -> dict:
    """Return a copy of the current agent state for the dashboard."""
    return dict(_agent_state)


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
    _init_client()
    
    if _trading_client is None:
        return "❌ TopstepX trading not available. Set TOPSTEPX_JWT_TOKEN in .env"
    
    account_id = await _ensure_practice_account()
    if account_id is None:
        return "❌ No practice account found"
    
    if quantity <= 0:
        return "❌ Quantity must be positive"

    # Hedging guard: reject buy if ANY open position is SHORT
    positions = await _trading_client._account_client.get_positions(account_id)
    for pos in positions:
        if pos.quantity < 0:
            return (
                f"❌ BLOCKED: You have a SHORT position ({abs(int(pos.quantity))}x {pos.symbol}). "
                f"Cannot place a BUY order while short. Use topstepx_close() to close "
                f"your short position first, then enter a new long."
            )

    # Never add to a losing long position
    for pos in positions:
        if pos.quantity > 0 and pos.unrealized_pnl < 0:
            return (
                f"❌ BLOCKED: Your LONG {abs(int(pos.quantity))}x {pos.symbol} is losing "
                f"(P&L: ${pos.unrealized_pnl:+,.2f}). Never add to a loser. "
                f"Cut the loss with topstepx_close() or wait for it to turn green."
            )

    # Place market buy order
    logger.info(f"🔵 EXECUTING BUY ORDER: {quantity}x {contract}")
    result = await _trading_client.place_market_order(
        account_id=account_id,
        contract_id=contract,
        side=OrderSide.BUY,
        size=quantity,
    )
    
    if result.get("success"):
        order_id = result.get("orderId", "unknown")
        logger.info(f"✅ BUY ORDER SUCCESSFUL: {quantity}x {contract} | Order ID: {order_id}")
        return (
            f"✓ BUY order placed successfully\n"
            f"  Contract: {contract}\n"
            f"  Quantity: {quantity}\n"
            f"  Order ID: {order_id}\n"
            f"  Account: {account_id}"
        )
    else:
        error = result.get("error", "Unknown error")
        logger.error(f"❌ BUY ORDER FAILED: {quantity}x {contract} | Error: {error}")
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
    _init_client()
    
    if _trading_client is None:
        return "❌ TopstepX trading not available. Set TOPSTEPX_JWT_TOKEN in .env"
    
    account_id = await _ensure_practice_account()
    if account_id is None:
        return "❌ No practice account found"
    
    if quantity <= 0:
        return "❌ Quantity must be positive"

    # Hedging guard: reject sell if ANY open position is LONG
    positions = await _trading_client._account_client.get_positions(account_id)
    for pos in positions:
        if pos.quantity > 0:
            return (
                f"❌ BLOCKED: You have a LONG position ({int(pos.quantity)}x {pos.symbol}). "
                f"Cannot place a SELL order while long. Use topstepx_close() to close "
                f"your long position first, then enter a new short."
            )

    # Never add to a losing short position
    for pos in positions:
        if pos.quantity < 0 and pos.unrealized_pnl < 0:
            return (
                f"❌ BLOCKED: Your SHORT {abs(int(pos.quantity))}x {pos.symbol} is losing "
                f"(P&L: ${pos.unrealized_pnl:+,.2f}). Never add to a loser. "
                f"Cut the loss with topstepx_close() or wait for it to turn green."
            )

    # Place market sell order
    logger.info(f"🔴 EXECUTING SELL ORDER: {quantity}x {contract}")
    result = await _trading_client.place_market_order(
        account_id=account_id,
        contract_id=contract,
        side=OrderSide.SELL,
        size=quantity,
    )
    
    if result.get("success"):
        order_id = result.get("orderId", "unknown")
        logger.info(f"✅ SELL ORDER SUCCESSFUL: {quantity}x {contract} | Order ID: {order_id}")
        return (
            f"✓ SELL order placed successfully\n"
            f"  Contract: {contract}\n"
            f"  Quantity: {quantity}\n"
            f"  Order ID: {order_id}\n"
            f"  Account: {account_id}"
        )
    else:
        error = result.get("error", "Unknown error")
        logger.error(f"❌ SELL ORDER FAILED: {quantity}x {contract} | Error: {error}")
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
    _init_client()

    if _trading_client is None:
        return "❌ TopstepX trading not available"

    account_id = await _ensure_practice_account()
    if account_id is None:
        return "❌ No practice account found"

    # Look up the current position
    positions = await _trading_client._account_client.get_positions(account_id)

    if not positions:
        logger.warning(f"⛔ CLOSE BLOCKED: No positions at all, but LLM tried to close {contract}")
        return "STOP: You have ZERO open positions. There is nothing to close. Do NOT call topstepx_close."

    target = None
    for pos in positions:
        if pos.symbol == contract:
            target = pos
            break

    if target is None:
        held = ", ".join(f"{pos.symbol} qty={int(pos.quantity)}" for pos in positions)
        logger.warning(f"⛔ CLOSE BLOCKED: LLM tried to close {contract} but only holding [{held}]")
        return f"STOP: You do NOT hold {contract}. You only hold: {held}. Do NOT close what you don't have."

    pos_size = abs(int(target.quantity))
    if pos_size == 0:
        return f"❌ Position size is zero for {contract}"

    close_qty = quantity if quantity > 0 else pos_size

    if close_qty >= pos_size:
        # Full close
        logger.info(f"🔶 CLOSING FULL POSITION: {pos_size}x {contract}")
        result = await _trading_client.close_position(account_id, contract)
        if result.get("success"):
            logger.info(f"✅ POSITION CLOSED: {pos_size}x {contract}")
            return (
                f"✓ Position CLOSED\n"
                f"  Contract: {contract}\n"
                f"  Quantity closed: {pos_size}\n"
                f"  Account: {account_id}"
            )
        else:
            error = result.get("error", "Unknown error")
            logger.error(f"❌ CLOSE FAILED: {contract} | Error: {error}")
            return f"❌ Close failed: {error}"
    else:
        # Partial close
        logger.info(f"🔶 PARTIAL CLOSE: {close_qty}/{pos_size}x {contract}")
        result = await _trading_client.partial_close_position(account_id, contract, close_qty)
        if result.get("success"):
            logger.info(f"✅ PARTIAL CLOSE: {close_qty}x {contract} (remaining: {pos_size - close_qty})")
            return (
                f"✓ Position PARTIALLY CLOSED\n"
                f"  Contract: {contract}\n"
                f"  Quantity closed: {close_qty}\n"
                f"  Remaining: {pos_size - close_qty}\n"
                f"  Account: {account_id}"
            )
        else:
            error = result.get("error", "Unknown error")
            logger.error(f"❌ PARTIAL CLOSE FAILED: {contract} | Error: {error}")
            return f"❌ Partial close failed: {error}"


_sentiment_cache: dict = {"time": 0.0}


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

    # ── Rescue values from wrong arg names the LLM may use ──
    if not reasoning and kwargs:
        reasoning = str(next(iter(kwargs.values()), ""))
    if sentiment == "neutral" and kwargs:
        for v in kwargs.values():
            if isinstance(v, str) and v.strip().lower() in ("bullish", "bearish"):
                sentiment = v.strip().lower()
                break

    # ── Throttle when flat: only update once per candle cycle (60s) ──
    now = _time.time()
    cache = _portfolio_cache
    if not cache.get("has_positions") and (now - _sentiment_cache["time"]) < 55.0:
        return "Recorded. STOP — do not call any more tools this turn."
    _sentiment_cache["time"] = now

    # ── Use cached portfolio state (already fetched by topstepx_portfolio or market connector)
    if cache.get("has_positions"):
        portfolio_prefix = cache.get("prefix", "[Unknown] ")
    else:
        portfolio_prefix = "[Flat] "

    # Fetch fresh positions only when we have positions (for accurate PnL prefix)
    if cache.get("has_positions"):
        actual_pnl_by_contract: dict[str, float] = {}
        actual_positions: dict[str, int] = {}
        try:
            account_id = await _ensure_practice_account()
            if account_id and _trading_client:
                _trading_client._account_client._accounts_cache = None
                summary = await _trading_client.get_account_summary(account_id)
                if "error" not in summary:
                    for pos in summary.get("positions", []):
                        actual_positions[pos["symbol"]] = int(pos["quantity"])
                        pnl = pos.get("unrealizedPnL", 0.0)
                        actual_pnl_by_contract[pos["symbol"]] = pnl
            if actual_positions:
                parts = []
                for contract, qty in actual_positions.items():
                    direction = "LONG" if qty > 0 else "SHORT"
                    pnl = actual_pnl_by_contract.get(contract, 0.0)
                    parts.append(f"{direction} {abs(qty)}x {contract} P&L: ${pnl:+,.2f}")
                portfolio_prefix = f"[{', '.join(parts)}] "
            else:
                portfolio_prefix = "[Flat] "
                _portfolio_cache["has_positions"] = False
        except Exception:
            pass

    sentiment_lower = sentiment.strip().lower()
    if sentiment_lower not in ("bullish", "bearish", "neutral"):
        sentiment_lower = "neutral"

    # ── Update dashboard with ground-truth + filtered reasoning ──
    display_reasoning = _filter_position_hallucinations(reasoning)
    _agent_state["latest_reasoning"] = portfolio_prefix + display_reasoning
    _agent_state["sentiment"] = sentiment_lower
    _agent_state["last_active"] = datetime.now().isoformat()

    logger.info(f"Sentiment ACCEPTED: {sentiment_lower} | {portfolio_prefix}| Display: {_truncate(display_reasoning, 150)}")
    return f"Recorded sentiment: {sentiment_lower}"


_portfolio_cache: dict = {"result": None, "time": 0.0, "has_positions": False}


@agent_tool
async def topstepx_portfolio(ctx: ToolContext) -> str:
    """Get portfolio status with real-time P&L.

    Returns:
        Portfolio summary with live position data
    """
    _init_client()

    if _trading_client is None:
        return "❌ TopstepX trading not available. Set TOPSTEPX_JWT_TOKEN in .env"

    account_id = await _ensure_practice_account()
    if account_id is None:
        return "❌ No practice account found"

    # When flat, return cached result for 30s to avoid spamming API
    import time as _time
    now = _time.time()
    cache = _portfolio_cache
    if (
        cache["result"] is not None
        and not cache["has_positions"]
        and (now - cache["time"]) < 30.0
    ):
        logger.debug(f"📊 PORTFOLIO (cached flat): returning cached result")
        return cache["result"]

    logger.info(f"📊 CHECKING PORTFOLIO STATUS for account {account_id}")

    # Invalidate cache so we always get fresh positions with live prices
    _trading_client._account_client._accounts_cache = None
    summary = await _trading_client.get_account_summary(account_id)

    if "error" in summary:
        return f"❌ {summary['error']}"

    positions = summary.get("positions", [])
    balance = summary["balance"]

    if not positions:
        logger.info(f"💼 PORTFOLIO: No open positions | Balance: ${balance:,.2f}")
        _agent_state["last_active"] = datetime.now().isoformat()
        result = (
            f"YOU HOLD: nothing — 0 open positions.\n"
            f"Balance: ${balance:,.2f} | Equity: ${balance:,.2f}\n"
            f"---\nNOW call report_sentiment(reasoning=<your analysis>, sentiment=<bullish|bearish|neutral>)"
        )
        _portfolio_cache.update(result=result, time=now, has_positions=False)
        return result

    # Build a blunt one-line summary the model can't miss
    total_pnl = 0.0
    hold_parts = []
    pos_lines = []

    for pos in positions:
        qty = pos["quantity"]
        direction = "LONG" if qty > 0 else "SHORT"
        abs_qty = abs(int(qty))
        avg = pos["avgPrice"]
        pnl = pos["unrealizedPnL"]
        total_pnl += pnl

        hold_parts.append(f"{direction} {abs_qty}x {pos['symbol']} (P&L: ${pnl:+,.2f})")
        pos_lines.append(
            f"  {pos['symbol']}: {direction} {abs_qty} @ ${avg:,.2f} | P&L: ${pnl:+,.2f}"
        )
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

    _agent_state["last_active"] = datetime.now().isoformat()
    result = "\n".join(lines)
    _portfolio_cache.update(result=result, time=now, has_positions=True)
    return result


# ── Main service ─────────────────────────────────────────────────


async def main():
    """Main entry point - deploy TopstepX trading tools."""
    parser = argparse.ArgumentParser(description="Deploy TopstepX trading tools")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap servers",
    )
    args = parser.parse_args()
    
    # Authenticate: use existing JWT or obtain one from username + API key
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
                logger.error("Authentication with API key failed")
                sys.exit(1)
            os.environ["TOPSTEPX_JWT_TOKEN"] = jwt_token
            logger.info("Authenticated successfully — JWT token obtained")
        else:
            logger.error(
                "TopstepX authentication required. Either:\n"
                "  1. Set TOPSTEPX_JWT_TOKEN, OR\n"
                "  2. Set TOPSTEPX_USERNAME and TOPSTEPX_API_KEY"
            )
            sys.exit(1)

    # Initialize client
    _init_client()
    if _trading_client:
        await _ensure_practice_account()
        if _practice_account_id is None:
            logger.error("No practice account found. Cannot enable trading.")
            sys.exit(1)

    # Start real-time price streaming via SignalR gateway
    price_streamer = None
    jwt_token = os.getenv("TOPSTEPX_JWT_TOKEN", "")
    stream_symbols_str = os.getenv("TOPSTEPX_STREAM_SYMBOLS", "").strip()
    if stream_symbols_str:
        stream_symbols = [s.strip() for s in stream_symbols_str.split(",") if s.strip()]
    elif _trading_client and _practice_account_id:
        # Auto-detect symbols from open positions
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
        logger.info(f"Price streamer started for: {stream_symbols}")

    print("=" * 60)
    print("TopstepX Trading Tools Deployment")
    print("=" * 60)
    
    # Initialize Kafka
    print(f"\nConnecting to Kafka at {args.bootstrap_servers}...")
    broker = BrokerClient(bootstrap_servers=args.bootstrap_servers)
    service = NodesService(broker)

    # Subscribe to futures price updates from market-connector
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
    print("\nRegistering TopstepX trading tools:")
    tools = [topstepx_buy, topstepx_sell, topstepx_close, topstepx_portfolio, report_sentiment]
    for tool in tools:
        service.register_node(tool)
        print(f"  ✓ {tool.tool_schema.name} - {tool.tool_schema.description}")
    
    print(f"\n✓ Trading enabled on practice account: {_practice_account_id}")
    print("\nTools are ready for agent requests!")
    print("Agents can now call:")
    print("  - topstepx_buy(contract, quantity)")
    print("  - topstepx_sell(contract, quantity)")
    print("  - topstepx_portfolio()")
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
            trading_client=_trading_client,
            get_account_id=lambda: _practice_account_id,
            get_agent_state=get_agent_state,
            set_account_id=_set_account_id,
        )

        # Pre-bind socket with SO_REUSEADDR to survive fast container
        # restarts under host networking. Retry if port is still held.
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
        if price_streamer:
            await price_streamer.stop()
        if _trading_client:
            await _trading_client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTopstepX trading tools stopped.")
