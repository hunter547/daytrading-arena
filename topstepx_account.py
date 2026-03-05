"""
TopstepX Account API client.

Fetches real account data from TopstepX API including:
- Account summaries
- Positions
- Balance information
- Performance metrics
"""

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)


@dataclass
class TopstepXPosition:
    """Represents a position in a TopstepX account."""
    symbol: str
    quantity: float
    avg_price: float
    market_value: float
    unrealized_pnl: float
    realized_pnl: float = 0.0


@dataclass
class TopstepXAccount:
    """Represents a TopstepX trading account."""
    account_id: str
    name: str
    balance: float
    equity: float
    buying_power: float
    positions: list[TopstepXPosition] = field(default_factory=list)
    daily_pnl: float = 0.0
    total_pnl: float = 0.0
    trade_count: int = 0
    last_updated: Optional[datetime] = None


class TopstepXAccountClient:
    """Client for fetching TopstepX account data via REST API."""
    
    # Class-level cache for current market prices (shared across instances)
    _current_prices: dict[str, float] = {}
    # Class-level cache for contract specs (tickSize, tickValue)
    _contract_specs: dict[str, dict[str, float]] = {}
    
    @classmethod
    def update_market_price(cls, symbol: str, price: float) -> None:
        """Update the current market price for a symbol.
        
        This is used to calculate unrealized PnL for positions.
        Should be called whenever a new quote is received.
        
        Args:
            symbol: Contract ID (e.g., "CON.F.US.MES.H26")
            price: Current market price
        """
        cls._current_prices[symbol] = price
        logger.debug(f"Updated market price: {symbol} = ${price:,.2f}")
    
    @classmethod
    def get_market_price(cls, symbol: str) -> Optional[float]:
        """Get the current market price for a symbol.
        
        Args:
            symbol: Contract ID
            
        Returns:
            Current price or None if not available
        """
        return cls._current_prices.get(symbol)

    @classmethod
    def update_contract_specs(cls, contract_id: str, tick_size: float, tick_value: float) -> None:
        """Update cached contract specs for a contract.

        Args:
            contract_id: Contract ID (e.g., "CON.F.US.MES.H26")
            tick_size: Minimum price increment
            tick_value: Dollar value of one tick
        """
        cls._contract_specs[contract_id] = {"tickSize": tick_size, "tickValue": tick_value}
        logger.debug(f"Updated contract specs: {contract_id} tickSize={tick_size}, tickValue={tick_value}")

    @classmethod
    def get_contract_specs(cls, contract_id: str) -> Optional[dict[str, float]]:
        """Get cached contract specs for a contract.

        Args:
            contract_id: Contract ID

        Returns:
            Dict with tickSize and tickValue, or None if not cached
        """
        return cls._contract_specs.get(contract_id)

    def __init__(
        self,
        jwt_token: str,
        api_base_url: str = "https://api.topstepx.com",
        timeout: float = 30.0,
    ):
        """Initialize TopstepX account client.
        
        Args:
            jwt_token: JWT authentication token
            api_base_url: Base URL for TopstepX API
            timeout: Request timeout in seconds
        """
        self._token = jwt_token
        self._api_base = api_base_url.rstrip('/')
        self._http_client = httpx.AsyncClient(
            timeout=timeout,
            headers={"Authorization": f"Bearer {jwt_token}"}
        )
        # Parse allowed account IDs from env var at init time
        allowed_ids_str = os.getenv("TOPSTEPX_ACCOUNT_IDS", "").strip()
        self._allowed_account_ids: set[str] | None = (
            {aid.strip() for aid in allowed_ids_str.split(",") if aid.strip()}
            if allowed_ids_str else None
        )
    
    async def get_accounts(
        self, 
        only_active: bool = True,
    ) -> list[TopstepXAccount]:
        """Fetch all accounts for the authenticated user.
        
        Automatically filters accounts based on their major loss limits:
        - 50K accounts: Must have ≥$48,000 (MLL: $2,000)
        - 100K accounts: Must have ≥$97,000 (MLL: $3,000)
        - 150K accounts: Must have ≥$145,500 (MLL: $4,500)
        - Practice accounts (PRAC-*): Always included
        
        Args:
            only_active: If True, only return active accounts (default: True)
        
        Returns:
            List of TopstepX accounts that are eligible for trading
        """
        try:
            url = f"{self._api_base}/api/Account/search"
            payload = {"onlyActiveAccounts": only_active}
            logger.debug(f"Fetching accounts from {url} with payload: {payload}")
            
            response = await self._http_client.post(url, json=payload)
            response.raise_for_status()
            
            data = response.json()
            
            if not data.get("success"):
                error_msg = data.get("errorMessage", "Unknown error")
                error_code = data.get("errorCode", -1)
                logger.error(f"Failed to fetch accounts: [{error_code}] {error_msg}")
                return []
            
            accounts_data = data.get("accounts", [])

            # Filter by TOPSTEPX_ACCOUNT_IDS early to skip unnecessary work
            if self._allowed_account_ids:
                accounts_data = [a for a in accounts_data if str(a.get("id", "")) in self._allowed_account_ids]
                logger.info(f"Filtered to allowed account IDs: {self._allowed_account_ids}")

            accounts = []
            for acc_data in accounts_data:
                # Determine major loss limit based on account type
                account_name = acc_data.get("name", "")
                balance = acc_data.get("balance", 0.0)

                # Practice accounts (PRAC-*) always eligible
                if "PRAC" in account_name:
                    account = await self._fetch_account_details(acc_data)
                    if account:
                        accounts.append(account)
                    continue
                
                # Determine starting balance and major loss limit from account name
                # Account naming: {SIZE}K{TYPE}-{VERSION}-{USER}-{ID}
                # Examples: 50KTC-V2-..., 100KTC-V2-..., 150KTC-V2-...
                starting_balance = 0
                major_loss_limit_amount = 0
                
                if account_name.startswith("50K"):
                    starting_balance = 50000
                    major_loss_limit_amount = 2000  # $2K MLL
                elif account_name.startswith("100K"):
                    starting_balance = 100000
                    major_loss_limit_amount = 3000  # $3K MLL
                elif account_name.startswith("150K"):
                    starting_balance = 150000
                    major_loss_limit_amount = 4500  # $4.5K MLL
                else:
                    # Unknown account type - include it and let canTrade decide
                    logger.warning(f"Unknown account type for {account_name}, including anyway")
                    account = await self._fetch_account_details(acc_data)
                    if account:
                        accounts.append(account)
                    continue
                
                # Calculate minimum allowed balance
                min_balance = starting_balance - major_loss_limit_amount
                
                # Check if account is above major loss limit
                if balance < min_balance:
                    logger.info(
                        f"Skipping account {account_name} (ID: {acc_data.get('id')}): "
                        f"Balance ${balance:,.2f} below MLL threshold ${min_balance:,.2f} "
                        f"(Starting: ${starting_balance:,.2f}, MLL: ${major_loss_limit_amount:,.2f})"
                    )
                    continue
                
                # Account is eligible
                account = await self._fetch_account_details(acc_data)
                if account:
                    accounts.append(account)
            
            logger.info(f"Fetched {len(accounts)} account(s)")
            return accounts
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching accounts: {e.response.status_code}")
            logger.error(f"Response: {e.response.text}")
            return []
        except Exception as e:
            logger.error(f"Error fetching accounts: {e}")
            return []
    
    async def _fetch_account_details(self, account_basic: dict[str, Any]) -> Optional[TopstepXAccount]:
        """Fetch detailed account information including positions.
        
        Args:
            account_basic: Basic account info from search endpoint
            
        Returns:
            TopstepXAccount with positions or None if failed
        """
        try:
            account_id = account_basic.get("id")
            name = account_basic.get("name", f"Account {account_id}")
            can_trade = account_basic.get("canTrade", False)
            is_visible = account_basic.get("isVisible", True)
            
            # Debug: Log account_basic to see what fields are available
            logger.info(f"🔍 Account {account_id} ({name}) basic info: canTrade={can_trade}, isVisible={is_visible}, startingDayBalance={account_basic.get('startingDayBalance', 'N/A')}")
            
            # Filter out accounts that can't trade or aren't visible
            if not account_id or not is_visible or not can_trade:
                logger.info(f"⏭️ Skipping account {account_id} ({name}): canTrade={can_trade}, isVisible={is_visible}")
                return None
            
            # Fetch positions for this account
            raw_positions = await self._get_raw_positions(account_id)

            # Fetch current prices and contract specs for any contracts missing from the cache
            for pos_data in raw_positions:
                cid = pos_data.get("contractId")
                if cid:
                    if self.get_market_price(cid) is None:
                        await self._fetch_current_price(cid)
                    if self.get_contract_specs(cid) is None:
                        await self._fetch_contract_specs(cid)

            positions = [p for p in (self._parse_position(d) for d in raw_positions) if p]
            
            # Calculate account metrics from positions
            unrealized_pnl = sum(pos.unrealized_pnl for pos in positions)
            realized_pnl = sum(pos.realized_pnl for pos in positions)
            
            # Get account balance from basic info
            # TopstepX returns "balance" field in the search endpoint
            balance = float(account_basic.get("balance", 0.0))
            logger.info(f"💰 Account {account_id} balance from API: ${balance:,.2f} | Unrealized PnL: ${unrealized_pnl:,.2f} | Equity: ${balance + unrealized_pnl:,.2f}")
            
            # Equity = Cash Balance + Unrealized PnL from positions
            equity = balance + unrealized_pnl
            
            return TopstepXAccount(
                account_id=str(account_id),
                name=name,
                balance=balance,
                equity=equity,
                buying_power=balance,  # Simplified
                positions=positions,
                daily_pnl=0.0,  # Would need historical data
                total_pnl=realized_pnl + unrealized_pnl,
                trade_count=0,  # Would need to fetch from trade history
                last_updated=datetime.now(),
            )
            
        except Exception as e:
            logger.error(f"Error fetching account details: {e}")
            return None
    
    async def get_account_by_id(self, account_id: int | str) -> Optional[TopstepXAccount]:
        """Fetch a specific account by ID.
        
        Args:
            account_id: Account ID to fetch
            
        Returns:
            TopstepX account or None if not found
        """
        try:
            # Search for all accounts and find the matching one
            accounts = await self.get_accounts(only_active=False)
            for account in accounts:
                if str(account.account_id) == str(account_id):
                    return account
            
            logger.warning(f"Account {account_id} not found")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching account {account_id}: {e}")
            return None
    
    async def _get_raw_positions(self, account_id: int | str) -> list[dict]:
        """Fetch raw position data from the API.

        Args:
            account_id: Account ID to fetch positions for

        Returns:
            List of raw position dicts from the API
        """
        try:
            url = f"{self._api_base}/api/Position/searchOpen"
            payload = {"accountId": int(account_id)}
            logger.debug(f"Fetching positions for account {account_id}")

            response = await self._http_client.post(url, json=payload)
            response.raise_for_status()

            data = response.json()

            if not data.get("success"):
                error_msg = data.get("errorMessage", "Unknown error")
                error_code = data.get("errorCode", -1)
                logger.error(f"Failed to fetch positions: [{error_code}] {error_msg}")
                return []

            return data.get("positions", [])

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching positions: {e.response.status_code}")
            logger.error(f"Response: {e.response.text}")
            return []
        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            return []

    async def get_positions(self, account_id: int | str) -> list[TopstepXPosition]:
        """Fetch positions for a specific account.

        Args:
            account_id: Account ID to fetch positions for

        Returns:
            List of positions
        """
        raw = await self._get_raw_positions(account_id)
        positions = [p for p in (self._parse_position(d) for d in raw) if p]
        logger.debug(f"Fetched {len(positions)} position(s) for account {account_id}")
        return positions
    

    
    def _parse_position(self, data: dict[str, Any]) -> Optional[TopstepXPosition]:
        """Parse position data from API response.
        
        Args:
            data: Position data dictionary from API
            
        Returns:
            Parsed TopstepXPosition or None if invalid
        """
        try:
            # TopstepX Position API response structure:
            # {
            #   "id": number,
            #   "accountId": number,
            #   "contractId": string,  # e.g., "CON.F.US.MES.H26"
            #   "creationTimestamp": string,
            #   "type": PositionTypeEnum (1=Long, 2=Short),
            #   "size": number,
            #   "averagePrice": number
            # }
            
            contract_id = data.get("contractId")
            if not contract_id:
                logger.warning("Position data missing contractId field")
                return None
            
            size = float(data.get("size", 0.0))
            avg_price = float(data.get("averagePrice", 0.0))
            position_type = data.get("type", 1)  # 1=Long, 2=Short
            
            # DEBUG: Log raw API data to verify type mapping
            logger.info(f"🔍 API POSITION: contract={contract_id}, type={position_type}, size={size}, avgPrice={avg_price}")
            
            # Get current market price for PnL calculation
            current_price = self.get_market_price(contract_id)
            
            if current_price:
                # Calculate unrealized PnL using tick-based futures formula:
                # PnL = (price_diff / tickSize) * tickValue * size
                price_diff = current_price - avg_price
                specs = self.get_contract_specs(contract_id)
                if specs:
                    tick_size = specs["tickSize"]
                    tick_value = specs["tickValue"]
                    unrealized_pnl = (price_diff / tick_size) * tick_value * size
                    logger.info(f"   💰 PnL CALC: current=${current_price:,.2f}, avg=${avg_price:,.2f}, diff={price_diff:,.2f}, tickSize={tick_size}, tickValue={tick_value}, size={size}, PnL=${unrealized_pnl:,.2f}")
                else:
                    # Fallback if contract specs not available
                    unrealized_pnl = price_diff * size
                    logger.warning(f"   ⚠️  No contract specs for {contract_id}, using simple PnL: ${unrealized_pnl:,.2f}")
                market_value = current_price * size
            else:
                # No current price available - use avg_price as fallback
                market_value = size * avg_price
                unrealized_pnl = 0.0
                logger.warning(f"   ⚠️  No current price for {contract_id}, PnL = $0.00")
            
            realized_pnl = 0.0
            
            # Adjust size for short positions (make negative)
            if position_type == 2:  # Short
                size = -abs(size)
            
            return TopstepXPosition(
                symbol=contract_id,
                quantity=size,
                avg_price=avg_price,
                market_value=market_value,
                unrealized_pnl=unrealized_pnl,
                realized_pnl=realized_pnl,
            )
            
        except Exception as e:
            logger.error(f"Error parsing position data: {e}")
            logger.debug(f"Position data: {data}")
            return None
    
    async def _fetch_current_price(self, contract_id: str) -> Optional[float]:
        """Fetch the current price for a contract via the History API.

        Retrieves the most recent 1-minute bar and returns its close price.
        Updates the in-memory price cache on success.
        """
        try:
            url = f"{self._api_base}/api/History/retrieveBars"
            now = datetime.now(timezone.utc)
            payload = {
                "contractId": contract_id,
                "live": False,
                "startTime": (now - timedelta(minutes=5)).isoformat(),
                "endTime": now.isoformat(),
                "unit": 2,  # Minute
                "unitNumber": 1,
                "limit": 1,
                "includePartialBar": True,
            }
            response = await self._http_client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()

            if data.get("success"):
                bars = data.get("bars", [])
                if bars:
                    price = float(bars[-1]["c"])
                    self.update_market_price(contract_id, price)
                    return price
        except Exception as e:
            logger.debug(f"Failed to fetch current price for {contract_id}: {e}")
        return None

    async def _fetch_contract_specs(self, contract_id: str) -> Optional[dict[str, float]]:
        """Fetch contract specs (tickSize, tickValue) from the Contract API.

        Updates the class-level cache on success.

        Args:
            contract_id: Contract ID (e.g., "CON.F.US.MES.H26")

        Returns:
            Dict with tickSize and tickValue, or None if failed
        """
        try:
            url = f"{self._api_base}/api/Contract/searchById"
            payload = {"contractId": contract_id}
            response = await self._http_client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()

            if data.get("success"):
                contract = data.get("contract", {})
                tick_size = float(contract.get("tickSize", 0))
                tick_value = float(contract.get("tickValue", 0))
                if tick_size > 0 and tick_value > 0:
                    self.update_contract_specs(contract_id, tick_size, tick_value)
                    logger.info(f"Fetched contract specs for {contract_id}: tickSize={tick_size}, tickValue={tick_value}")
                    return {"tickSize": tick_size, "tickValue": tick_value}
                else:
                    logger.warning(f"Invalid contract specs for {contract_id}: tickSize={tick_size}, tickValue={tick_value}")
            else:
                error_msg = data.get("errorMessage", "Unknown error")
                logger.error(f"Failed to fetch contract specs for {contract_id}: {error_msg}")
        except Exception as e:
            logger.debug(f"Failed to fetch contract specs for {contract_id}: {e}")
        return None

    async def close(self):
        """Close HTTP client."""
        await self._http_client.aclose()


class TopstepXPriceStreamer:
    """Streams real-time prices from TopstepX SignalR gateway.

    Connects to the market hub and subscribes to GatewayQuote events,
    updating TopstepXAccountClient._current_prices on each tick.
    """

    def __init__(self, jwt_token: str, symbols: list[str], ws_base: str = "https://rtc.topstepx.com"):
        self._jwt_token = jwt_token
        self._symbols = symbols
        self._hub_url = f"{ws_base}/hubs/market?access_token={jwt_token}"
        self._connection = None

    def _handle_trade(self, args) -> None:
        """Handle GatewayTrade callback from SignalR.

        GatewayTrade payload (list of trades):
        [{ symbolId, price, timestamp, type, volume }, ...]
        The contract_id comes as the first SignalR argument.
        """
        try:
            if isinstance(args, list) and len(args) >= 2:
                contract_id, data = args[0], args[1]
            else:
                logger.debug(f"Unexpected trade args format: {args}")
                return

            trades = data if isinstance(data, list) else [data]
            for trade in trades:
                price = trade.get("price")
                if price is not None:
                    TopstepXAccountClient.update_market_price(contract_id, float(price))
        except Exception as e:
            logger.error(f"Error handling price trade: {e}")

    async def start(self) -> None:
        """Start the SignalR connection and subscribe to trades."""
        from signalrcore.hub_connection_builder import HubConnectionBuilder

        logger.info(f"Starting price streamer for {self._symbols}")

        self._connection = (
            HubConnectionBuilder()
            .with_url(
                self._hub_url,
                options={
                    "skip_negotiation": True,
                    "access_token_factory": lambda: self._jwt_token,
                    "headers": {"Authorization": f"Bearer {self._jwt_token}"},
                },
            )
            .build()
        )

        def _on_open():
            logger.info("Price streamer SignalR connected")
            for symbol in self._symbols:
                self._connection.invoke("SubscribeContractTrades", [symbol])
                logger.info(f"Price streamer subscribed to trades: {symbol}")

        self._connection.on("GatewayTrade", self._handle_trade)
        self._connection.on_open(_on_open)
        self._connection.on_close(lambda: logger.warning("Price streamer SignalR disconnected, falling back to History API"))
        self._connection.on_error(lambda data: logger.error(f"Price streamer SignalR error: {data}"))

        self._connection.start()

    async def stop(self) -> None:
        """Stop the SignalR connection."""
        if self._connection:
            try:
                self._connection.stop()
            except Exception as e:
                logger.debug(f"Error stopping price streamer: {e}")


async def main():
    """CLI tool for viewing TopstepX account data."""
    import argparse
    import os
    import sys
    from dotenv import load_dotenv
    
    load_dotenv()
    
    parser = argparse.ArgumentParser(
        description="View TopstepX account data"
    )
    parser.add_argument(
        "--token",
        type=str,
        help="JWT token (or set TOPSTEPX_JWT_TOKEN env var)",
    )
    parser.add_argument(
        "--account-id",
        type=str,
        help="Specific account ID to fetch (optional)",
    )
    parser.add_argument(
        "--api-url",
        type=str,
        default="https://api.topstepx.com",
        help="TopstepX API base URL",
    )
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    
    # Get token
    token = args.token or os.getenv("TOPSTEPX_JWT_TOKEN")
    if not token:
        # Try to authenticate
        from topstepx_auth import authenticate_topstepx
        
        username = os.getenv("TOPSTEPX_USERNAME")
        api_key = os.getenv("TOPSTEPX_API_KEY")
        
        if username and api_key:
            logger.info("No JWT token found, authenticating...")
            token = await authenticate_topstepx(
                username, 
                api_key,
                environment=os.getenv("TOPSTEPX_ENVIRONMENT", "topstepx"),
                api_base_url=args.api_url,
            )
            
            if not token:
                logger.error("Authentication failed")
                sys.exit(1)
        else:
            logger.error(
                "JWT token required. Either:\n"
                "  1. Set TOPSTEPX_JWT_TOKEN, OR\n"
                "  2. Set TOPSTEPX_USERNAME and TOPSTEPX_API_KEY\n"
                "  3. Pass --token JWT_TOKEN"
            )
            sys.exit(1)
    
    client = TopstepXAccountClient(jwt_token=token, api_base_url=args.api_url)
    
    try:
        if args.account_id:
            # Fetch specific account
            account = await client.get_account_by_id(args.account_id)
            if account:
                print(f"\n{'='*70}")
                print(f"Account: {account.name} ({account.account_id})")
                print(f"{'='*70}")
                print(f"Balance:       ${account.balance:,.2f}")
                print(f"Equity:        ${account.equity:,.2f}")
                print(f"Buying Power:  ${account.buying_power:,.2f}")
                print(f"Daily P&L:     ${account.daily_pnl:+,.2f}")
                print(f"Total P&L:     ${account.total_pnl:+,.2f}")
                print(f"Trade Count:   {account.trade_count}")
                
                if account.positions:
                    print(f"\nPositions ({len(account.positions)}):")
                    print(f"{'Symbol':<15} {'Qty':>10} {'Avg Price':>12} {'Value':>15} {'P&L':>15}")
                    print("-" * 70)
                    for pos in account.positions:
                        print(
                            f"{pos.symbol:<15} {pos.quantity:>10.2f} "
                            f"${pos.avg_price:>11,.2f} ${pos.market_value:>14,.2f} "
                            f"${pos.unrealized_pnl:>+14,.2f}"
                        )
            else:
                logger.error(f"Account {args.account_id} not found")
        else:
            # Fetch all accounts
            accounts = await client.get_accounts()
            
            if not accounts:
                print("\n❌ No accounts found or authentication failed")
                sys.exit(1)
            
            print(f"\n{'='*70}")
            print(f"TopstepX Accounts ({len(accounts)})")
            print(f"{'='*70}\n")
            
            for i, account in enumerate(accounts, 1):
                print(f"{i}. {account.name} ({account.account_id})")
                print(f"   Balance:       ${account.balance:,.2f}")
                print(f"   Equity:        ${account.equity:,.2f}")
                print(f"   Buying Power:  ${account.buying_power:,.2f}")
                print(f"   Daily P&L:     ${account.daily_pnl:+,.2f}")
                print(f"   Total P&L:     ${account.total_pnl:+,.2f}")
                print(f"   Positions:     {len(account.positions)}")
                print(f"   Trades:        {account.trade_count}")
                print()
            
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
