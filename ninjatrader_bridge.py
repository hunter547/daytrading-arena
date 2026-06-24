"""
NinjaTrader Bridge copy-trading adapter.

Mirrors the promoted agent's simulated TopstepX trades onto a real NinjaTrader 8
account via the NinjaTrader Bridge REST API (http://localhost:5000/docs/v1).

Three layers:

1. ``NinjaTraderBridgeClient`` — a thin async httpx wrapper over the bridge REST
   endpoints we need (accounts, positions, trades, instruments, order, flatten).

2. ``NinjaTraderContractMapper`` — maps a TopstepX ``contract_id`` such as
   ``CON.F.US.MES.M26`` to the NinjaTrader front-month instrument name such as
   ``MES 09-26``. Most base tickers are identical between the two platforms; the
   handful that differ (TopstepX ``EP``/``ENQ`` vs NinjaTrader ``ES``/``NQ``) are
   handled by ``TICKER_OVERRIDES``. Front-month resolution always uses whatever
   contract NinjaTrader currently lists for that ticker, so expiry rolls are
   handled automatically.

3. ``NinjaTraderCopyTrader`` — a high-level adapter whose methods line up
   one-to-one with the promoted agent's tool calls (buy / sell / close /
   portfolio / available contracts), translating each into the matching bridge
   API call against a single configured account (e.g. ``TOF130830``).

Design notes:
- Order execution failures NEVER raise into the caller — they return a result
  dict with ``success: False`` so the sim trade is never blocked by the mirror.
- The mapper caches resolved instrument names with a TTL because the front-month
  contract rolls over periodically.
"""

import asyncio
import logging
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


# TopstepX base ticker -> NinjaTrader base ticker, for the cases where they differ.
# Everything not listed here is assumed identical (MES, MNQ, M2K, RTY, CL, MCL,
# GC, MGC, SI, HG, MBT, MET, NKD, ZC, ZW, ZS, ZN, ZB, ... all match directly).
TICKER_OVERRIDES: dict[str, str] = {
    "EP": "ES",    # E-mini S&P 500
    "ENQ": "NQ",   # E-mini NASDAQ 100
}


def extract_base_ticker(contract_id: str) -> str:
    """Extract the base ticker from a TopstepX contract_id.

    ``CON.F.US.MES.M26`` -> ``MES``. If the id has no recognizable structure the
    whole string is returned so the caller can still attempt a lookup.
    """
    parts = contract_id.split(".")
    if len(parts) >= 2:
        # Last segment is the expiry code (e.g. M26); the one before is the ticker.
        return parts[-2]
    return contract_id


class NinjaTraderBridgeClient:
    """Async client for the NinjaTrader Bridge REST API."""

    def __init__(self, base_url: str = "http://localhost:5000", timeout: float = 10.0):
        self._base_url = base_url.rstrip("/")
        # http.sys rejects requests whose Host header doesn't match the URL
        # reservation; the bridge is reserved under "localhost". Always send
        # Host: localhost so requests are accepted regardless of routing
        # (localhost vs host.docker.internal vs LAN IP).
        from urllib.parse import urlparse
        parsed = urlparse(self._base_url)
        host_header = f"localhost:{parsed.port}" if parsed.port else "localhost"
        self._http = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=timeout,
            headers={"Content-Type": "application/json", "Host": host_header},
        )

    async def health(self) -> dict:
        resp = await self._http.get("/api/v1/health")
        resp.raise_for_status()
        return resp.json()

    async def get_accounts(self) -> list[dict]:
        resp = await self._http.get("/api/v1/accounts")
        resp.raise_for_status()
        return resp.json()

    async def get_positions(self, account: Optional[str] = None) -> list[dict]:
        params = {"account": account} if account else {}
        resp = await self._http.get("/api/v1/positions", params=params)
        resp.raise_for_status()
        return resp.json()

    async def get_trades(
        self, account: Optional[str] = None, instrument: Optional[str] = None
    ) -> list[dict]:
        params = {}
        if account:
            params["account"] = account
        if instrument:
            params["instrument"] = instrument
        resp = await self._http.get("/api/v1/trades", params=params)
        resp.raise_for_status()
        return resp.json()

    async def get_instruments(
        self,
        type: Optional[str] = None,
        exchange: Optional[str] = None,
        ticker: Optional[str] = None,
        query: Optional[str] = None,
    ) -> list[dict]:
        params = {}
        if type:
            params["type"] = type
        if exchange:
            params["exchange"] = exchange
        if ticker:
            params["ticker"] = ticker
        if query:
            params["q"] = query
        resp = await self._http.get("/api/v1/instruments", params=params)
        resp.raise_for_status()
        return resp.json()

    async def place_order(
        self,
        account: str,
        instrument: str,
        action: str,
        quantity: int = 1,
        order_type: str = "Market",
        limit_price: float = 0,
        stop_price: float = 0,
    ) -> dict:
        payload = {
            "account": account,
            "instrument": instrument,
            "action": action,
            "quantity": quantity,
            "orderType": order_type,
            "limitPrice": limit_price,
            "stopPrice": stop_price,
        }
        resp = await self._http.post("/api/v1/order", json=payload)
        resp.raise_for_status()
        return resp.json()

    async def flatten(self, account: str, instrument: Optional[str] = None) -> dict:
        payload = {"account": account}
        if instrument:
            payload["instrument"] = instrument
        resp = await self._http.post("/api/v1/flatten", json=payload)
        resp.raise_for_status()
        return resp.json()

    async def close(self) -> None:
        await self._http.aclose()


class NinjaTraderContractMapper:
    """Resolve TopstepX contract_ids to NinjaTrader front-month instrument names."""

    def __init__(self, client: NinjaTraderBridgeClient, cache_ttl: float = 3600.0):
        self._client = client
        self._cache_ttl = cache_ttl
        # base ticker -> (timestamp, instrument_name)
        self._cache: dict[str, tuple[float, str]] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def ninjatrader_ticker(contract_id: str) -> str:
        """TopstepX contract_id -> NinjaTrader base ticker (applying overrides)."""
        base = extract_base_ticker(contract_id)
        return TICKER_OVERRIDES.get(base, base)

    async def resolve(self, contract_id: str) -> Optional[str]:
        """Return the NinjaTrader front-month instrument name (e.g. ``MES 09-26``).

        Returns None when no matching instrument is found on the bridge.
        """
        ticker = self.ninjatrader_ticker(contract_id)

        now = time.monotonic()
        cached = self._cache.get(ticker)
        if cached and (now - cached[0]) < self._cache_ttl:
            return cached[1]

        async with self._lock:
            # Re-check after acquiring the lock (another coroutine may have filled it)
            cached = self._cache.get(ticker)
            if cached and (time.monotonic() - cached[0]) < self._cache_ttl:
                return cached[1]

            try:
                instruments = await self._client.get_instruments(type="Future", ticker=ticker)
            except Exception as e:
                logger.error(f"NT instrument lookup failed for {ticker}: {e}")
                # Serve a stale cache entry if we have one rather than failing hard.
                return cached[1] if cached else None

            # The ticker filter is a substring match, so pick the EXACT ticker.
            match = next((i for i in instruments if i.get("ticker") == ticker), None)
            if match is None and instruments:
                # No exact match — fall back to the first result only if it shares
                # the ticker prefix, otherwise give up.
                logger.warning(
                    f"No exact NT ticker match for '{ticker}' "
                    f"(got {[i.get('ticker') for i in instruments]})"
                )
                return None

            if match is None:
                logger.warning(f"No NT instrument found for ticker '{ticker}' "
                               f"(from contract {contract_id})")
                return None

            name = match.get("name")
            if not name:
                return None
            self._cache[ticker] = (time.monotonic(), name)
            logger.info(f"Mapped TopstepX {contract_id} -> NinjaTrader '{name}'")
            return name


class NinjaTraderCopyTrader:
    """High-level copy-trader: mirrors promoted-agent actions onto a NT account."""

    def __init__(
        self,
        client: NinjaTraderBridgeClient,
        mapper: NinjaTraderContractMapper,
        account: str,
    ):
        self._client = client
        self._mapper = mapper
        self.account = account

    # ── Order mirroring (matches buy / sell / close tool calls) ──────────

    async def mirror_buy(self, contract_id: str, quantity: int) -> dict:
        return await self._market_order(contract_id, "Buy", quantity)

    async def mirror_sell(self, contract_id: str, quantity: int) -> dict:
        return await self._market_order(contract_id, "Sell", quantity)

    async def _market_order(self, contract_id: str, action: str, quantity: int) -> dict:
        instrument = await self._mapper.resolve(contract_id)
        if not instrument:
            return {"success": False, "error": f"No NinjaTrader instrument for {contract_id}"}
        try:
            result = await self._client.place_order(
                account=self.account,
                instrument=instrument,
                action=action,
                quantity=int(quantity),
                order_type="Market",
            )
            logger.info(f"NT {action} {quantity}x {instrument} on {self.account}: {result}")
            return {"success": True, "instrument": instrument, "result": result}
        except httpx.HTTPStatusError as e:
            body = e.response.text if e.response is not None else ""
            return {"success": False, "instrument": instrument,
                    "error": f"HTTP {e.response.status_code if e.response else '?'}: {body[:200]}"}
        except Exception as e:
            return {"success": False, "instrument": instrument, "error": str(e)}

    async def mirror_close(self, contract_id: str, quantity: int = 0) -> dict:
        """Close (quantity 0 or >= held) or partially close a position.

        For a full close we use the bridge ``flatten`` endpoint. For a partial
        close we read the current NinjaTrader position to determine direction and
        place an offsetting market order sized to ``quantity``.
        """
        instrument = await self._mapper.resolve(contract_id)
        if not instrument:
            return {"success": False, "error": f"No NinjaTrader instrument for {contract_id}"}

        # Determine current NT position for this instrument.
        held_qty = await self._position_quantity(instrument)

        # Full close: quantity 0, or >= what we hold, or we can't read the position.
        if quantity <= 0 or held_qty is None or abs(quantity) >= abs(held_qty):
            try:
                result = await self._client.flatten(self.account, instrument)
                logger.info(f"NT flatten {instrument} on {self.account}: {result}")
                return {"success": True, "instrument": instrument, "result": result}
            except Exception as e:
                return {"success": False, "instrument": instrument, "error": str(e)}

        # Partial close: offset in the opposite direction of the held position.
        if held_qty == 0:
            return {"success": False, "instrument": instrument, "error": "No NT position to close"}
        action = "Sell" if held_qty > 0 else "Buy"
        try:
            result = await self._client.place_order(
                account=self.account,
                instrument=instrument,
                action=action,
                quantity=int(abs(quantity)),
                order_type="Market",
            )
            logger.info(f"NT partial close {action} {quantity}x {instrument}: {result}")
            return {"success": True, "instrument": instrument, "result": result}
        except Exception as e:
            return {"success": False, "instrument": instrument, "error": str(e)}

    async def _position_quantity(self, instrument: str) -> Optional[int]:
        """Signed quantity NinjaTrader currently holds for ``instrument``.

        Positive = long, negative = short, 0 = flat, None = lookup failed.
        """
        try:
            positions = await self._client.get_positions(self.account)
        except Exception as e:
            logger.warning(f"NT position lookup failed for {instrument}: {e}")
            return None
        for pos in positions:
            if pos.get("instrument") == instrument:
                return _signed_position_quantity(pos)
        return 0

    # ── Read-only state for the dashboard (matches portfolio tool call) ──

    async def get_account_summary(self) -> dict:
        """Build a dashboard-friendly snapshot of the NinjaTrader account.

        Shape mirrors what the dashboard expects for an agent: balance, equity,
        positions (with unrealizedPnL), plus realized day P&L and win-rate stats
        derived from completed round-trip trades.
        """
        try:
            accounts = await self._client.get_accounts()
        except Exception as e:
            return {"error": f"NT accounts fetch failed: {e}"}

        acct = next((a for a in accounts if a.get("name") == self.account), None)
        if acct is None:
            return {"error": f"NinjaTrader account {self.account} not found"}

        balance = acct.get("cashValue", 0.0)
        realized = acct.get("realizedPnL", 0.0)
        unrealized = acct.get("unrealizedPnL", 0.0)

        try:
            raw_positions = await self._client.get_positions(self.account)
        except Exception:
            raw_positions = []

        positions = []
        for pos in raw_positions:
            qty = _signed_position_quantity(pos)
            if qty == 0:
                continue
            positions.append({
                "symbol": pos.get("instrument", "?"),
                "quantity": qty,
                "avgPrice": pos.get("averagePrice", pos.get("avgPrice", 0.0)),
                "unrealizedPnL": pos.get("unrealizedPnL", 0.0),
            })

        # Win rate + day realized P&L from completed trades this session.
        win_rate = 0.0
        total_trades = 0
        total_profit = 0.0
        total_loss = 0.0
        trade_realized = 0.0
        try:
            trades = await self._client.get_trades(self.account)
            total_trades = len(trades)
            wins = 0
            for t in trades:
                pnl = t.get("profitCurrency", 0.0)
                trade_realized += pnl
                if pnl >= 0:
                    wins += 1
                    total_profit += pnl
                else:
                    total_loss += pnl
            if total_trades:
                win_rate = wins / total_trades
        except Exception:
            pass

        # Prefer the account's realized P&L; fall back to summed trades.
        realized_day = realized if realized else trade_realized

        return {
            "account": self.account,
            "balance": balance,
            "equity": balance + unrealized,
            "unrealizedPnL": unrealized,
            "realizedDayPnl": realized_day,
            "positions": positions,
            "winRate": win_rate,
            "totalTrades": total_trades,
            "totalProfit": total_profit,
            "totalLoss": total_loss,
            "connection": acct.get("connection", ""),
            "status": acct.get("status", ""),
        }

    async def available_contracts(self) -> list[dict]:
        """List NinjaTrader futures instruments (matches available-contracts call)."""
        try:
            return await self._client.get_instruments(type="Future")
        except Exception as e:
            logger.error(f"NT available_contracts failed: {e}")
            return []

    async def close(self) -> None:
        await self._client.close()


def _signed_position_quantity(pos: dict) -> int:
    """Normalize a bridge position dict to a signed integer quantity.

    The bridge may express direction either via a signed ``quantity`` or via a
    ``marketPosition`` string ("Long"/"Short"/"Flat") with a magnitude quantity.
    """
    qty = pos.get("quantity", 0) or 0
    market_pos = str(pos.get("marketPosition", "")).lower()
    if market_pos == "short":
        return -abs(int(qty))
    if market_pos == "long":
        return abs(int(qty))
    if market_pos == "flat":
        return 0
    return int(qty)


async def create_copytrader(
    base_url: str,
    account: str,
    timeout: float = 10.0,
) -> Optional[NinjaTraderCopyTrader]:
    """Create and health-check a copy-trader. Returns None if the bridge is down."""
    client = NinjaTraderBridgeClient(base_url, timeout=timeout)
    try:
        health = await client.health()
        if health.get("status") != "ok":
            logger.warning(f"NinjaTrader bridge unhealthy: {health}")
    except Exception as e:
        logger.error(f"NinjaTrader bridge unreachable at {base_url}: {e}")
        await client.close()
        return None

    # Verify the target account exists and is connected.
    try:
        accounts = await client.get_accounts()
        acct = next((a for a in accounts if a.get("name") == account), None)
        if acct is None:
            logger.error(f"NinjaTrader account '{account}' not found on bridge — "
                         f"copy trading disabled")
            await client.close()
            return None
        if acct.get("status") != "Connected":
            logger.warning(f"NinjaTrader account '{account}' status is "
                           f"{acct.get('status')} (not Connected)")
    except Exception as e:
        logger.error(f"Failed to verify NinjaTrader account '{account}': {e}")
        await client.close()
        return None

    mapper = NinjaTraderContractMapper(client)
    logger.info(f"NinjaTrader copy-trader ready: bridge={base_url} account={account}")
    return NinjaTraderCopyTrader(client, mapper, account)
