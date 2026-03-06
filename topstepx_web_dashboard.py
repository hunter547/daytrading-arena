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
) -> FastAPI:
    """Create FastAPI app with shared trading state.

    Args:
        trading_client: TopstepXTradingClient instance (shared with Kafka workers)
        get_account_id: Callable returning the current practice account ID
        get_agent_state: Optional callable returning current agent activity state
    """
    app = FastAPI(title="TopstepX Dashboard", docs_url=None, redoc_url=None)

    view_only = os.getenv("DASHBOARD_VIEW_ONLY", "").lower() in ("1", "true", "yes")

    # Cache for WebSocket portfolio pushes (avoid hammering API)
    _portfolio_cache: dict = {"data": None, "ts": 0.0}
    CACHE_TTL = 2.0

    async def _get_portfolio() -> dict:
        """Get portfolio data, using cache if fresh."""
        now = time.monotonic()
        if _portfolio_cache["data"] and (now - _portfolio_cache["ts"]) < CACHE_TTL:
            return _portfolio_cache["data"]

        account_id = get_account_id()
        if account_id is None:
            return {"error": "No practice account"}

        summary = await trading_client.get_account_summary(account_id)
        prices = dict(TopstepXAccountClient._current_prices)
        result = {**summary, "prices": prices}
        _portfolio_cache["data"] = result
        _portfolio_cache["ts"] = now
        return result

    # ── Routes ────────────────────────────────────────────────────

    @app.get("/")
    async def serve_dashboard():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/api/config")
    async def api_config():
        return {"viewOnly": view_only}

    @app.get("/api/portfolio")
    async def api_portfolio():
        return await _get_portfolio()

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
        return result

    @app.post("/api/close")
    async def api_close(contract: str = Query(...)):
        """Close/flatten a position by sending an opposite-side market order."""
        if view_only:
            return JSONResponse({"success": False, "error": "Dashboard is in view-only mode"}, 403)
        account_id = get_account_id()
        if account_id is None:
            return JSONResponse({"success": False, "error": "No practice account"}, 400)

        # Look up the position to determine direction and size
        summary = await trading_client.get_account_summary(account_id)
        positions = summary.get("positions", [])
        target = None
        for pos in positions:
            if pos["symbol"] == contract:
                target = pos
                break

        if target is None:
            return JSONResponse({"success": False, "error": f"No open position for {contract}"}, 400)

        qty = target["quantity"]
        if qty == 0:
            return JSONResponse({"success": False, "error": "Position size is zero"}, 400)

        from topstepx_trading_tools import OrderSide

        # Opposite side to flatten
        side = OrderSide.SELL if qty > 0 else OrderSide.BUY
        size = abs(int(qty))

        result = await trading_client.place_market_order(
            account_id=account_id,
            contract_id=contract,
            side=side,
            size=size,
        )
        return result

    # ── WebSocket ─────────────────────────────────────────────────

    @app.websocket("/ws")
    async def ws_portfolio(ws: WebSocket):
        await ws.accept()
        logger.info("WebSocket client connected")
        try:
            while True:
                data = await _get_portfolio()
                if get_agent_state:
                    data["agent"] = get_agent_state()
                await ws.send_json(data)
                await asyncio.sleep(2)
        except WebSocketDisconnect:
            logger.info("WebSocket client disconnected")
        except Exception as e:
            logger.debug(f"WebSocket error: {e}")

    return app
