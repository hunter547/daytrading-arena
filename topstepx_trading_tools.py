"""
TopstepX Trading Tools for Agents

Provides @agent_tool functions that allow AI agents to execute real trades
on TopstepX practice accounts. Includes buy, sell, and portfolio query tools.

Usage:
    python topstepx_trading_tools.py --bootstrap-servers localhost:9092
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv

from calfkit.broker.broker import BrokerClient
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
    
    async def get_account_summary(self, account_id: int) -> dict:
        """Get account summary with positions.
        
        Args:
            account_id: Account ID to query
            
        Returns:
            Account summary dictionary
        """
        accounts = await self._account_client.get_accounts()
        for account in accounts:
            if str(account.account_id) == str(account_id):
                return {
                    "accountId": account.account_id,
                    "name": account.name,
                    "equity": account.equity,
                    "balance": account.balance,
                    "positions": [
                        {
                            "symbol": pos.symbol,
                            "quantity": pos.quantity,
                            "avgPrice": pos.avg_price,
                            "marketValue": pos.market_value,
                            "unrealizedPnL": pos.unrealized_pnl,
                        }
                        for pos in account.positions
                    ],
                }
        return {"error": f"Account {account_id} not found"}
    
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


async def _ensure_practice_account():
    """Ensure practice account ID is loaded."""
    global _practice_account_id
    
    if _trading_client is None:
        return None
    
    if _practice_account_id is None:
        _practice_account_id = await _trading_client.get_practice_account_id()
        if _practice_account_id:
            logger.info(f"Practice account ID: {_practice_account_id}")
        else:
            logger.warning("No practice account found")
    
    return _practice_account_id


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
async def topstepx_portfolio(ctx: ToolContext) -> str:
    """Get portfolio status.
    
    Returns:
        Portfolio summary
    """
    _init_client()
    
    if _trading_client is None:
        return "❌ TopstepX trading not available. Set TOPSTEPX_JWT_TOKEN in .env"
    
    account_id = await _ensure_practice_account()
    if account_id is None:
        return "❌ No practice account found"
    
    logger.info(f"📊 CHECKING PORTFOLIO STATUS for account {account_id}")
    summary = await _trading_client.get_account_summary(account_id)
    
    if "error" in summary:
        return f"❌ {summary['error']}"
    
    positions = summary.get("positions", [])
    
    if not positions:
        logger.info(f"💼 PORTFOLIO: No open positions | Equity: ${summary['equity']:,.2f}")
        return (
            f"📊 TopstepX Portfolio (Account: {summary['name']})\n"
            f"  Equity: ${summary['equity']:,.2f}\n"
            f"  Balance: ${summary['balance']:,.2f}\n"
            f"  Positions: None"
        )
    
    lines = [
        f"📊 TopstepX Portfolio (Account: {summary['name']})",
        f"  Equity: ${summary['equity']:,.2f}",
        f"  Balance: ${summary['balance']:,.2f}",
        f"  Positions:",
    ]
    
    for pos in positions:
        qty = pos['quantity']
        direction = "LONG" if qty > 0 else "SHORT"
        pnl = pos['unrealizedPnL']
        pnl_sign = "+" if pnl >= 0 else ""
        
        position_str = f"{pos['symbol']}: {direction} {abs(qty)} @ ${pos['avgPrice']:,.2f} (P&L: {pnl_sign}${pnl:,.2f})"
        logger.info(f"💼 DISPLAYING: {position_str} (qty={qty})")
        
        lines.append(f"    {position_str}")
    
    return "\n".join(lines)


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
    
    # Check for JWT token
    if not os.getenv("TOPSTEPX_JWT_TOKEN"):
        logger.error(
            "TOPSTEPX_JWT_TOKEN not set. Please set it in .env file.\n"
            "Run: python topstepx_auth.py to get a token"
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
        price_streamer = TopstepXPriceStreamer(jwt_token, stream_symbols, ws_base=ws_base)
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

    # Register tools
    print("\nRegistering TopstepX trading tools:")
    tools = [topstepx_buy, topstepx_sell, topstepx_portfolio]
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
    
    try:
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
