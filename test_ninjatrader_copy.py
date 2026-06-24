"""
Tests for the NinjaTrader copy-trading adapter (ninjatrader_bridge.py).

Covers the two pieces with real logic:
- NinjaTraderContractMapper: TopstepX contract_id -> NinjaTrader front-month name
- NinjaTraderCopyTrader: buy/sell/close translation onto a single account

Uses a FakeBridgeClient so no network or running bridge is required.

Run:  python test_ninjatrader_copy.py
  or: python -m pytest test_ninjatrader_copy.py -v   (if pytest is installed)
"""

import asyncio

from ninjatrader_bridge import (
    NinjaTraderContractMapper,
    NinjaTraderCopyTrader,
    extract_base_ticker,
    _signed_position_quantity,
)


# ── Fake bridge client ────────────────────────────────────────────

class FakeBridgeClient:
    """In-memory stand-in for NinjaTraderBridgeClient."""

    # NinjaTrader front-month instruments keyed by ticker.
    INSTRUMENTS = {
        "MES": {"ticker": "MES", "name": "MES 09-26", "type": "Future"},
        "ES":  {"ticker": "ES",  "name": "ES 09-26",  "type": "Future"},
        "NQ":  {"ticker": "NQ",  "name": "NQ 09-26",  "type": "Future"},
        "MNQ": {"ticker": "MNQ", "name": "MNQ 09-26", "type": "Future"},
        "MGC": {"ticker": "MGC", "name": "MGC 08-26", "type": "Future"},
    }

    def __init__(self, account="TOF130830", positions=None, accounts=None, trades=None):
        self.account = account
        self._positions = positions or []
        self._accounts = accounts or [
            {"name": account, "status": "Connected", "connection": "Top One Futures",
             "cashValue": 98698.48, "realizedPnL": 0.0, "unrealizedPnL": 0.0},
        ]
        self._trades = trades or []
        self.orders = []   # records of place_order calls
        self.flattens = []  # records of flatten calls

    async def get_instruments(self, type=None, exchange=None, ticker=None, query=None):
        # Emulate the bridge's substring ticker match: return everything whose
        # ticker contains the query substring.
        if ticker:
            return [v for k, v in self.INSTRUMENTS.items() if ticker in k]
        return list(self.INSTRUMENTS.values())

    async def get_accounts(self):
        return self._accounts

    async def get_positions(self, account=None):
        return self._positions

    async def get_trades(self, account=None, instrument=None):
        return self._trades

    async def place_order(self, account, instrument, action, quantity=1,
                          order_type="Market", limit_price=0, stop_price=0):
        rec = {"account": account, "instrument": instrument, "action": action,
               "quantity": quantity, "orderType": order_type}
        self.orders.append(rec)
        return {"status": "submitted", **rec}

    async def flatten(self, account, instrument=None):
        rec = {"account": account, "instrument": instrument}
        self.flattens.append(rec)
        return {"status": "flattened", **rec}

    async def close(self):
        pass


# ── extract_base_ticker / ticker mapping ──────────────────────────

def test_extract_base_ticker():
    assert extract_base_ticker("CON.F.US.MES.M26") == "MES"
    assert extract_base_ticker("CON.F.US.EP.U26") == "EP"
    assert extract_base_ticker("CON.F.US.ENQ.Z25") == "ENQ"
    # Degenerate inputs don't crash.
    assert extract_base_ticker("MES") == "MES"


def test_ninjatrader_ticker_overrides():
    fn = NinjaTraderContractMapper.ninjatrader_ticker
    # Direct (identical) tickers pass through.
    assert fn("CON.F.US.MES.M26") == "MES"
    assert fn("CON.F.US.MNQ.M26") == "MNQ"
    # E-mini S&P / NASDAQ differ between platforms.
    assert fn("CON.F.US.EP.M26") == "ES"
    assert fn("CON.F.US.ENQ.M26") == "NQ"


def test_signed_position_quantity():
    assert _signed_position_quantity({"quantity": 3}) == 3
    assert _signed_position_quantity({"quantity": -2}) == -2
    assert _signed_position_quantity({"marketPosition": "Long", "quantity": 2}) == 2
    assert _signed_position_quantity({"marketPosition": "Short", "quantity": 2}) == -2
    assert _signed_position_quantity({"marketPosition": "Flat", "quantity": 0}) == 0


# ── Contract mapper resolution ────────────────────────────────────

def test_mapper_resolves_front_month():
    async def run():
        client = FakeBridgeClient()
        mapper = NinjaTraderContractMapper(client)
        assert await mapper.resolve("CON.F.US.MES.M26") == "MES 09-26"
        # Override: EP -> ES front month.
        assert await mapper.resolve("CON.F.US.EP.M26") == "ES 09-26"
        assert await mapper.resolve("CON.F.US.ENQ.M26") == "NQ 09-26"
    asyncio.run(run())


def test_mapper_exact_ticker_match_not_substring():
    """ticker=ES must resolve to ES, not MES (substring would match both)."""
    async def run():
        client = FakeBridgeClient()
        mapper = NinjaTraderContractMapper(client)
        # 'ES' is a substring of 'MES'; resolver must pick the exact 'ES' row.
        assert await mapper.resolve("CON.F.US.EP.M26") == "ES 09-26"
    asyncio.run(run())


def test_mapper_unknown_ticker_returns_none():
    async def run():
        client = FakeBridgeClient()
        mapper = NinjaTraderContractMapper(client)
        assert await mapper.resolve("CON.F.US.ZZZ.M26") is None
    asyncio.run(run())


def test_mapper_caches_resolution():
    async def run():
        client = FakeBridgeClient()
        calls = {"n": 0}
        orig = client.get_instruments

        async def counting(*a, **k):
            calls["n"] += 1
            return await orig(*a, **k)

        client.get_instruments = counting
        mapper = NinjaTraderContractMapper(client)
        await mapper.resolve("CON.F.US.MES.M26")
        await mapper.resolve("CON.F.US.MES.Z26")  # same ticker, different expiry
        assert calls["n"] == 1  # second call served from cache
    asyncio.run(run())


# ── Copy-trader order translation ─────────────────────────────────

def _trader(client):
    return NinjaTraderCopyTrader(client, NinjaTraderContractMapper(client), client.account)


def test_mirror_buy_places_market_order():
    async def run():
        client = FakeBridgeClient()
        trader = _trader(client)
        res = await trader.mirror_buy("CON.F.US.MES.M26", 2)
        assert res["success"] is True
        assert res["instrument"] == "MES 09-26"
        assert len(client.orders) == 1
        o = client.orders[0]
        assert o == {"account": "TOF130830", "instrument": "MES 09-26",
                     "action": "Buy", "quantity": 2, "orderType": "Market"}
    asyncio.run(run())


def test_mirror_sell_places_market_order():
    async def run():
        client = FakeBridgeClient()
        trader = _trader(client)
        res = await trader.mirror_sell("CON.F.US.ENQ.M26", 1)
        assert res["success"] is True
        assert client.orders[0]["instrument"] == "NQ 09-26"
        assert client.orders[0]["action"] == "Sell"
    asyncio.run(run())


def test_mirror_buy_unknown_contract_fails_gracefully():
    async def run():
        client = FakeBridgeClient()
        trader = _trader(client)
        res = await trader.mirror_buy("CON.F.US.ZZZ.M26", 1)
        assert res["success"] is False
        assert "No NinjaTrader instrument" in res["error"]
        assert client.orders == []  # never placed
    asyncio.run(run())


def test_mirror_close_full_uses_flatten():
    async def run():
        client = FakeBridgeClient(positions=[
            {"instrument": "MES 09-26", "marketPosition": "Long", "quantity": 3,
             "averagePrice": 5800, "unrealizedPnL": 50},
        ])
        trader = _trader(client)
        res = await trader.mirror_close("CON.F.US.MES.M26", 0)  # 0 = close all
        assert res["success"] is True
        assert client.flattens == [{"account": "TOF130830", "instrument": "MES 09-26"}]
        assert client.orders == []  # full close uses flatten, not an order
    asyncio.run(run())


def test_mirror_close_partial_offsets_long():
    async def run():
        client = FakeBridgeClient(positions=[
            {"instrument": "MES 09-26", "marketPosition": "Long", "quantity": 5,
             "averagePrice": 5800, "unrealizedPnL": 0},
        ])
        trader = _trader(client)
        res = await trader.mirror_close("CON.F.US.MES.M26", 2)  # partial
        assert res["success"] is True
        # Long position partially closed -> Sell 2.
        assert client.orders == [{"account": "TOF130830", "instrument": "MES 09-26",
                                  "action": "Sell", "quantity": 2, "orderType": "Market"}]
        assert client.flattens == []
    asyncio.run(run())


def test_mirror_close_partial_offsets_short():
    async def run():
        client = FakeBridgeClient(positions=[
            {"instrument": "MNQ 09-26", "marketPosition": "Short", "quantity": 4,
             "averagePrice": 20000, "unrealizedPnL": 0},
        ])
        trader = _trader(client)
        res = await trader.mirror_close("CON.F.US.MNQ.M26", 1)
        assert res["success"] is True
        # Short position partially closed -> Buy 1.
        assert client.orders[0]["action"] == "Buy"
        assert client.orders[0]["quantity"] == 1
    asyncio.run(run())


def test_mirror_close_qty_ge_held_uses_flatten():
    async def run():
        client = FakeBridgeClient(positions=[
            {"instrument": "MES 09-26", "marketPosition": "Long", "quantity": 2},
        ])
        trader = _trader(client)
        # Requesting to close more than held -> flatten the whole thing.
        res = await trader.mirror_close("CON.F.US.MES.M26", 5)
        assert res["success"] is True
        assert len(client.flattens) == 1
        assert client.orders == []
    asyncio.run(run())


# ── Account summary for dashboard ─────────────────────────────────

def test_get_account_summary_shape():
    async def run():
        client = FakeBridgeClient(
            accounts=[{"name": "TOF130830", "status": "Connected",
                       "connection": "Top One Futures", "cashValue": 98700.0,
                       "realizedPnL": -250.0, "unrealizedPnL": 120.0}],
            positions=[{"instrument": "NQ 09-26", "marketPosition": "Long",
                        "quantity": 1, "averagePrice": 29667.0, "unrealizedPnL": 120.0}],
            trades=[
                {"profitCurrency": -975.0}, {"profitCurrency": 300.0},
                {"profitCurrency": 425.0},
            ],
        )
        trader = _trader(client)
        s = await trader.get_account_summary()
        assert s["account"] == "TOF130830"
        assert s["balance"] == 98700.0
        assert s["equity"] == 98700.0 + 120.0
        assert s["unrealizedPnL"] == 120.0
        assert s["realizedDayPnl"] == -250.0  # prefers account realizedPnL
        assert s["totalTrades"] == 3
        assert abs(s["winRate"] - (2 / 3)) < 1e-9
        assert len(s["positions"]) == 1
        assert s["positions"][0]["symbol"] == "NQ 09-26"
        assert s["positions"][0]["quantity"] == 1
    asyncio.run(run())


def test_get_account_summary_missing_account():
    async def run():
        client = FakeBridgeClient(accounts=[{"name": "OTHER", "status": "Connected"}])
        trader = _trader(client)
        s = await trader.get_account_summary()
        assert "error" in s
    asyncio.run(run())


def test_get_account_summary_skips_flat_positions():
    async def run():
        client = FakeBridgeClient(positions=[
            {"instrument": "MES 09-26", "marketPosition": "Flat", "quantity": 0},
        ])
        trader = _trader(client)
        s = await trader.get_account_summary()
        assert s["positions"] == []
    asyncio.run(run())


def _run_all():
    """Minimal test runner so the suite works without pytest installed."""
    import sys
    import traceback

    tests = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    passed = failed = 0
    for fn in tests:
        try:
            fn()
            print(f"  PASS  {fn.__name__}")
            passed += 1
        except Exception:
            print(f"  FAIL  {fn.__name__}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    import sys
    sys.exit(_run_all())
