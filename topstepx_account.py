"""
TopstepX Account API client.

Fetches real account data from TopstepX API including:
- Account summaries
- Positions
- Balance information
- Performance metrics
"""

import asyncio
import json
import logging
import os
import time as _time
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
    can_trade: bool = True
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
        # Cache for get_accounts() to avoid hammering the API
        self._accounts_cache: list[TopstepXAccount] | None = None
        self._accounts_cache_ts: float = 0.0
        self._accounts_cache_ttl: float = 10.0  # seconds
    
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
        now = _time.monotonic()
        if self._accounts_cache is not None and (now - self._accounts_cache_ts) < self._accounts_cache_ttl:
            return self._accounts_cache

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

            # Auto-select practice account (PRAC-*) if one exists
            practice_accounts = [a for a in accounts_data if "PRAC" in a.get("name", "")]
            if practice_accounts:
                accounts_data = practice_accounts
                logger.info(f"Auto-selected practice account: {practice_accounts[0].get('name')} (ID: {practice_accounts[0].get('id')})")

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
            self._accounts_cache = accounts
            self._accounts_cache_ts = _time.monotonic()
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
            
            # Filter out accounts that aren't visible or have no ID
            # Note: canTrade is False outside market hours, so we don't filter on it
            # for practice accounts — we still want to show their balance/positions.
            if not account_id or not is_visible:
                logger.info(f"⏭️ Skipping account {account_id} ({name}): isVisible={is_visible}")
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
                can_trade=can_trade,
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
            
            # Adjust size for short positions (make negative) BEFORE PnL calc
            if position_type == 2:  # Short
                size = -abs(size)

            # Get current market price for PnL calculation
            current_price = self.get_market_price(contract_id)

            if current_price:
                # Calculate unrealized PnL using tick-based futures formula:
                # PnL = (price_diff / tickSize) * tickValue * signed_size
                # For longs: price up = positive PnL. For shorts: price up = negative PnL.
                price_diff = current_price - avg_price
                specs = self.get_contract_specs(contract_id)
                if specs:
                    tick_size = specs["tickSize"]
                    tick_value = specs["tickValue"]
                    unrealized_pnl = (price_diff / tick_size) * tick_value * size
                    logger.info(f"   💰 PnL CALC: current=${current_price:,.2f}, avg=${avg_price:,.2f}, diff={price_diff:,.2f}, tickSize={tick_size}, tickValue={tick_value}, size={size}, PnL=${unrealized_pnl:,.2f}")
                else:
                    unrealized_pnl = price_diff * size
                    logger.warning(f"   ⚠️  No contract specs for {contract_id}, using simple PnL: ${unrealized_pnl:,.2f}")
                market_value = current_price * abs(size)
            else:
                market_value = abs(size) * avg_price
                unrealized_pnl = 0.0
                logger.warning(f"   ⚠️  No current price for {contract_id}, PnL = $0.00")

            realized_pnl = 0.0
            
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
    
    # Class-level rate limit backoff for retrieveBars
    _price_poll_backoff_until: float = 0.0

    async def _fetch_current_price(self, contract_id: str) -> Optional[float]:
        """Fetch the current price for a contract via the History API.

        Retrieves the most recent 1-minute bar and returns its close price.
        Updates the in-memory price cache on success.
        """
        import time as _time
        if _time.time() < TopstepXAccountClient._price_poll_backoff_until:
            return None  # still in backoff

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
            if response.status_code == 429:
                TopstepXAccountClient._price_poll_backoff_until = _time.time() + 60.0
                logger.warning("Price poll 429 — backing off 60s")
                return None
            response.raise_for_status()
            data = response.json()

            if data.get("success"):
                bars = data.get("bars", [])
                if bars:
                    price = float(bars[-1]["c"])
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
    """Streams real-time market prices via SignalR Market Hub.

    Uses SubscribeContractTrades on the Market Hub to get live price ticks.
    Also connects to User Hub for account/position/order notifications.
    """

    RECORD_SEPARATOR = "\x1e"

    def __init__(self, jwt_token: str, symbols: list[str], ws_base: str = "https://rtc.topstepx.com",
                 account_client: Optional['TopstepXAccountClient'] = None,
                 account_id: Optional[int] = None):
        self._jwt_token = jwt_token
        self._symbols = set(symbols)
        self._ws_base = ws_base
        self._account_client = account_client
        self._account_id = account_id
        self._market_task: Optional[asyncio.Task] = None
        self._user_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._market_ws = None  # live WebSocket for dynamic subscriptions
        self._sub_counter = 0   # invocation ID counter

    # ── Market Hub (live prices) ──────────────────────────────────

    async def _run_market_hub(self) -> None:
        """Connect to Market Hub and stream GatewayTrade events."""
        import websockets

        url = f"wss://{self._ws_base.split('://')[-1]}/hubs/market?access_token={self._jwt_token}"

        while not self._stop_event.is_set():
            try:
                async with websockets.connect(url) as ws:
                    # Handshake
                    await ws.send(json.dumps({"protocol": "json", "version": 1}) + self.RECORD_SEPARATOR)
                    hs = await ws.recv()
                    if "error" in hs:
                        logger.error(f"Market Hub handshake error: {hs}")
                        await asyncio.sleep(5)
                        continue

                    # Subscribe to each symbol
                    self._sub_counter = 0
                    for symbol in list(self._symbols):
                        self._sub_counter += 1
                        msg = json.dumps({
                            "type": 1,
                            "target": "SubscribeContractTrades",
                            "arguments": [symbol],
                            "invocationId": str(self._sub_counter),
                        }) + self.RECORD_SEPARATOR
                        await ws.send(msg)

                    self._market_ws = ws
                    logger.info(f"Market Hub connected — streaming {self._symbols}")

                    while not self._stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            continue

                        for chunk in raw.strip(self.RECORD_SEPARATOR).split(self.RECORD_SEPARATOR):
                            if not chunk:
                                continue
                            msg = json.loads(chunk)
                            msg_type = msg.get("type")

                            if msg_type == 1:  # Invocation — GatewayTrade
                                self._handle_gateway_trade(msg.get("arguments", []))
                            elif msg_type == 7:  # Close
                                logger.warning("Market Hub server closed connection")
                                raise ConnectionError("Server closed")
                            # type 3 = completion, type 6 = ping — ignore

            except asyncio.CancelledError:
                self._market_ws = None
                return
            except Exception as e:
                self._market_ws = None
                if not self._stop_event.is_set():
                    logger.warning(f"Market Hub disconnected: {e} — reconnecting in 2s")
                    await asyncio.sleep(2)

    def _handle_gateway_trade(self, args: list) -> None:
        """Handle GatewayTrade event: [contractId, [trade, trade, ...]]"""
        try:
            if len(args) < 2:
                return
            contract_id = args[0]
            trades = args[1]
            if isinstance(trades, list) and trades:
                latest = trades[-1]
                price = latest.get("price")
                if price is not None:
                    TopstepXAccountClient.update_market_price(contract_id, float(price))
        except Exception as e:
            logger.error(f"Error handling GatewayTrade: {e}")

    # ── User Hub (account/position/order events) ──────────────────

    async def _run_user_hub(self) -> None:
        """Connect to User Hub for account, position, order, and trade events."""
        import websockets

        if self._account_id is None:
            logger.warning("No account ID — skipping User Hub")
            return

        url = f"wss://{self._ws_base.split('://')[-1]}/hubs/user?access_token={self._jwt_token}"

        while not self._stop_event.is_set():
            try:
                async with websockets.connect(url) as ws:
                    await ws.send(json.dumps({"protocol": "json", "version": 1}) + self.RECORD_SEPARATOR)
                    hs = await ws.recv()
                    if "error" in hs:
                        logger.error(f"User Hub handshake error: {hs}")
                        await asyncio.sleep(5)
                        continue

                    # Subscribe to all user streams
                    for method, args in [
                        ("SubscribeAccounts", []),
                        ("SubscribeOrders", [self._account_id]),
                        ("SubscribePositions", [self._account_id]),
                        ("SubscribeTrades", [self._account_id]),
                    ]:
                        await ws.send(json.dumps({
                            "type": 1, "target": method,
                            "arguments": args, "invocationId": method,
                        }) + self.RECORD_SEPARATOR)

                    logger.info(f"User Hub connected — account {self._account_id}")

                    while not self._stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            continue

                        for chunk in raw.strip(self.RECORD_SEPARATOR).split(self.RECORD_SEPARATOR):
                            if not chunk:
                                continue
                            msg = json.loads(chunk)
                            msg_type = msg.get("type")

                            if msg_type == 1:
                                target = msg.get("target", "")
                                event_args = msg.get("arguments", [])
                                self._handle_user_event(target, event_args)
                            elif msg_type == 7:
                                logger.warning("User Hub server closed connection")
                                raise ConnectionError("Server closed")

            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._stop_event.is_set():
                    logger.warning(f"User Hub disconnected: {e} — reconnecting in 2s")
                    await asyncio.sleep(2)

    def _handle_user_event(self, target: str, args: list) -> None:
        """Dispatch User Hub events."""
        try:
            data = args[0] if args else {}
            if target == "GatewayUserAccount":
                if self._account_client:
                    self._account_client._accounts_cache = None
                logger.info(f"Account update: balance=${data.get('balance', 0):,.2f} canTrade={data.get('canTrade')}")
            elif target == "GatewayUserPosition":
                if self._account_client:
                    self._account_client._accounts_cache = None
                logger.info(f"Position update: {data.get('contractId')} size={data.get('size')} avg={data.get('averagePrice')}")
            elif target == "GatewayUserTrade":
                if self._account_client:
                    self._account_client._accounts_cache = None
                logger.info(f"User trade: {data.get('contractId')} @ ${data.get('price', 0):,.2f} pnl=${data.get('profitAndLoss', 0):,.2f}")
            elif target == "GatewayUserOrder":
                logger.info(f"Order update: {data.get('contractId')} status={data.get('status')}")
        except Exception as e:
            logger.error(f"Error handling {target}: {e}")

    # ── Public API ────────────────────────────────────────────────

    async def start(self) -> None:
        """Start both Market Hub (prices) and User Hub (account events)."""
        logger.info(f"Starting price streamer for {self._symbols}")
        self._market_task = asyncio.create_task(self._run_market_hub())
        self._user_task = asyncio.create_task(self._run_user_hub())
        # Start REST polling fallback for price updates
        self._poll_task: Optional[asyncio.Task] = asyncio.create_task(self._poll_prices_loop())

    def subscribe(self, contract_id: str) -> None:
        """Dynamically add a contract to the price streaming set.

        Called when an agent enters a position in a new contract so that
        live PnL calculations can occur. Sends SubscribeContractTrades
        on the live WebSocket if connected.
        """
        if contract_id not in self._symbols:
            self._symbols.add(contract_id)
            logger.info(f"Price streamer: subscribed to {contract_id} (total: {len(self._symbols)})")
            # Send subscription on live WebSocket
            if self._market_ws is not None:
                self._sub_counter += 1
                msg = json.dumps({
                    "type": 1,
                    "target": "SubscribeContractTrades",
                    "arguments": [contract_id],
                    "invocationId": str(self._sub_counter),
                }) + self.RECORD_SEPARATOR
                asyncio.ensure_future(self._market_ws.send(msg))

    def unsubscribe(self, contract_id: str) -> None:
        """Remove a contract from the price streaming set.

        Called when all positions in a contract are closed.
        Does NOT remove contracts from the initial set.
        """
        if contract_id in self._symbols:
            self._symbols.discard(contract_id)
            logger.info(f"Price streamer: unsubscribed from {contract_id} (total: {len(self._symbols)})")

    async def _poll_prices_loop(self) -> None:
        """REST polling fallback: fetch latest price for all subscribed symbols every 15s.

        This ensures live PnL works even when SignalR is broken.
        Uses 15s interval to stay well within the 50 req/30s rate limit.
        """
        while not self._stop_event.is_set():
            try:
                for symbol in list(self._symbols):
                    if self._account_client:
                        await self._account_client._fetch_current_price(symbol)
                    if self._stop_event.is_set():
                        break
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.debug(f"Price poll error: {e}")
            await asyncio.sleep(15)

    async def stop(self) -> None:
        """Stop all streaming."""
        self._stop_event.set()
        for task in (self._market_task, self._user_task, getattr(self, '_poll_task', None)):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass


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
