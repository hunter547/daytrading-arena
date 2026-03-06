"""
FastAPI web dashboard for TopstepX trading tools.

Provides REST endpoints and WebSocket for live portfolio monitoring
and manual trade execution from the browser.

Usage:
    app = create_app(trading_client, get_account_id)
    # Then run with uvicorn in the same event loop
"""

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import Callable, Optional

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from topstepx_account import TopstepXAccountClient

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


def create_app(
    trading_client,
    get_account_id: Callable[[], Optional[int]],
    get_agent_state: Optional[Callable[[], dict]] = None,
    set_account_id: Optional[Callable[[int], None]] = None,
) -> FastAPI:
    """Create FastAPI app with shared trading state.

    Args:
        trading_client: TopstepXTradingClient instance (shared with Kafka workers)
        get_account_id: Callable returning the current practice account ID
        get_agent_state: Optional callable returning current agent activity state
    """
    app = FastAPI(title="TopstepX Dashboard", docs_url=None, redoc_url=None)

    view_only = os.getenv("DASHBOARD_VIEW_ONLY", "").lower() in ("1", "true", "yes")

    # Cached account summary, refreshed in background
    _last_summary: dict = {"data": None}

    async def _refresh_loop():
        """Background task that refreshes account data every 10s."""
        while True:
            try:
                account_id = get_account_id()
                if account_id is not None:
                    summary = await trading_client.get_account_summary(account_id)
                    if "error" not in summary:
                        _last_summary["data"] = summary
                        # If the returned account differs from what we asked for,
                        # the old account was reset — update the global ID.
                        returned_id = summary.get("accountId")
                        if returned_id and str(returned_id) != str(account_id) and set_account_id:
                            set_account_id(int(returned_id))
            except Exception as e:
                logger.error(f"Background refresh error: {e}")
            await asyncio.sleep(10)

    @app.on_event("startup")
    async def _start_refresh():
        asyncio.create_task(_refresh_loop())

    def _build_snapshot() -> dict:
        """Build portfolio snapshot from cached summary + live prices. Non-blocking."""
        summary = _last_summary["data"]
        if summary is None:
            return {"error": "Loading account data..."}

        prices = dict(TopstepXAccountClient._current_prices)
        positions = [dict(p) for p in summary.get("positions", [])]

        for pos in positions:
            symbol = pos.get("symbol", "")
            live_price = prices.get(symbol)
            if live_price is not None and pos.get("avgPrice"):
                avg = pos["avgPrice"]
                qty = pos.get("quantity", 0)
                specs = TopstepXAccountClient.get_contract_specs(symbol)
                if specs:
                    tick_size = specs["tickSize"]
                    tick_value = specs["tickValue"]
                    pos["unrealizedPnL"] = ((live_price - avg) / tick_size) * tick_value * qty
                else:
                    pos["unrealizedPnL"] = (live_price - avg) * qty

        total_unrealized = sum(p.get("unrealizedPnL", 0) for p in positions)
        balance = summary.get("balance", 0)

        return {**summary, "equity": balance + total_unrealized, "positions": positions, "prices": prices}

    # ── Routes ────────────────────────────────────────────────────

    @app.get("/")
    async def serve_dashboard():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/api/config")
    async def api_config():
        return {"viewOnly": view_only}

    @app.get("/api/portfolio")
    async def api_portfolio():
        return _build_snapshot()

    @app.get("/api/prices")
    async def api_prices():
        return dict(TopstepXAccountClient._current_prices)

    @app.get("/api/agent")
    async def api_agent():
        if get_agent_state:
            return get_agent_state()
        return {}

    @app.post("/api/buy")
    async def api_buy(
        contract: str = Query(...),
        quantity: int = Query(..., gt=0),
    ):
        if view_only:
            return JSONResponse({"success": False, "error": "Dashboard is in view-only mode"}, 403)
        account_id = get_account_id()
        if account_id is None:
            return JSONResponse({"success": False, "error": "No practice account"}, 400)

        from topstepx_trading_tools import OrderSide

        result = await trading_client.place_market_order(
            account_id=account_id,
            contract_id=contract,
            side=OrderSide.BUY,
            size=quantity,
        )
        trading_client._account_client._accounts_cache = None  # Invalidate cache after trade
        return result

    @app.post("/api/sell")
    async def api_sell(
        contract: str = Query(...),
        quantity: int = Query(..., gt=0),
    ):
        if view_only:
            return JSONResponse({"success": False, "error": "Dashboard is in view-only mode"}, 403)
        account_id = get_account_id()
        if account_id is None:
            return JSONResponse({"success": False, "error": "No practice account"}, 400)

        from topstepx_trading_tools import OrderSide

        result = await trading_client.place_market_order(
            account_id=account_id,
            contract_id=contract,
            side=OrderSide.SELL,
            size=quantity,
        )
        trading_client._account_client._accounts_cache = None  # Invalidate cache after trade
        return result

    @app.post("/api/close")
    async def api_close(contract: str = Query(...)):
        """Close/flatten a position using the close API."""
        if view_only:
            return JSONResponse({"success": False, "error": "Dashboard is in view-only mode"}, 403)
        account_id = get_account_id()
        if account_id is None:
            return JSONResponse({"success": False, "error": "No practice account"}, 400)

        result = await trading_client.close_position(account_id, contract)
        trading_client._account_client._accounts_cache = None
        return result

    # ── WebSocket ─────────────────────────────────────────────────

    @app.websocket("/ws")
    async def ws_portfolio(ws: WebSocket):
        await ws.accept()
        logger.info("WebSocket client connected")
        try:
            while True:
                data = _build_snapshot()
                if get_agent_state:
                    data["agent"] = get_agent_state()
                await ws.send_json(data)
                await asyncio.sleep(1)
        except WebSocketDisconnect:
            logger.info("WebSocket client disconnected")
        except Exception as e:
            logger.error(f"WebSocket error: {e}", exc_info=True)

    return app
