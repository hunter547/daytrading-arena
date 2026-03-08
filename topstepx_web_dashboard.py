"""
FastAPI web dashboard for the TopstepX trading arena.

Provides REST endpoints and WebSocket for live portfolio monitoring.
Sim accounts are backed by MySQL; real practice account stats come from TopstepX API.

Usage:
    app = create_app(sim_manager, get_all_agents_state, agent_name, ...)
    # Then run with uvicorn in the same event loop
"""

import asyncio
import logging
import os
from pathlib import Path
from typing import Callable, Optional

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from sim_account_manager import SimAccountManager
from topstepx_account import TopstepXAccountClient
from topstepx_web_client import TopstepDashboardClient, TopstepXWebClient, WebTradingAccount

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


def create_app(
    sim_manager: SimAccountManager,
    get_all_agents_state: Optional[Callable[[], dict]] = None,
    agent_name: str = "futures-trader",
    mirror_agent: str = "",
    trading_client=None,
    get_account_id: Optional[Callable[[], Optional[int]]] = None,
    set_account_id: Optional[Callable[[int], None]] = None,
    web_client: Optional[TopstepXWebClient] = None,
    dashboard_client: Optional[TopstepDashboardClient] = None,
) -> FastAPI:
    """Create FastAPI app backed by SimAccountManager + optional TopstepX API.

    Args:
        sim_manager: SimAccountManager instance for simulated accounts
        get_all_agents_state: Optional callable returning {agent_name: state_dict, ...}
        agent_name: Default agent name for single-agent queries
        mirror_agent: Agent name whose trades are mirrored to TopstepX practice account;
                      only this agent's dashboard view uses TopstepX API data
        trading_client: Optional TopstepXTradingClient for real practice account
        get_account_id: Callable returning the current practice account ID
        set_account_id: Callable to update the practice account ID
        web_client: Optional TopstepXWebClient for richer practice account data
        dashboard_client: Optional TopstepDashboardClient for balance history
    """
    app = FastAPI(title="TopstepX Agent Arena", docs_url=None, redoc_url=None)

    view_only = os.getenv("DASHBOARD_VIEW_ONLY", "").lower() in ("1", "true", "yes")

    # Cached sim portfolio snapshots per agent
    _agent_snapshots: dict[str, dict] = {}
    # Cached real practice account data (only used for mirror_agent)
    _last_practice: dict = {"summary": None, "web_account": None}
    # Cached dashboard account ID
    _dash_account_id: dict = {"value": None}

    def _get_agent_names() -> list[str]:
        """Get all known agent names from activity state + sim snapshots."""
        names = set(_agent_snapshots.keys())
        if get_all_agents_state:
            names.update(get_all_agents_state().keys())
        # Only use fallback agent_name if no agents discovered yet
        if not names and agent_name:
            names.add(agent_name)
        return sorted(names)

    async def _refresh_loop():
        """Background task that refreshes all agents' portfolio data."""
        while True:
            try:
                # Refresh sim portfolios for all known agents
                for name in _get_agent_names():
                    try:
                        portfolio = await sim_manager.get_portfolio(name)
                        if "error" not in portfolio:
                            _agent_snapshots[name] = portfolio
                    except Exception as e:
                        logger.debug(f"Portfolio refresh error for {name}: {e}")

                # Refresh real practice account stats (only for mirror agent)
                if mirror_agent and trading_client and get_account_id:
                    account_id = get_account_id()
                    if account_id is not None:
                        summary = await trading_client.get_account_summary(account_id)
                        if "error" not in summary:
                            _last_practice["summary"] = summary
                            returned_id = summary.get("accountId")
                            if returned_id and str(returned_id) != str(account_id) and set_account_id:
                                set_account_id(int(returned_id))

                    if web_client:
                        try:
                            acct = await web_client.get_active_practice_account()
                            if acct:
                                _last_practice["web_account"] = acct
                        except Exception as e:
                            logger.debug(f"Web client refresh error: {e}")
            except Exception as e:
                logger.error(f"Background refresh error: {e}")
            await asyncio.sleep(5)

    @app.on_event("startup")
    async def _start_refresh():
        asyncio.create_task(_refresh_loop())

    def _build_agent_snapshot(name: str) -> dict:
        """Build portfolio snapshot for a single agent.

        For the mirror agent: uses real TopstepX practice account data exclusively
        (balance, equity, positions, stats) when available.
        For all other agents: uses sim account data from MySQL.
        """
        is_mirror = mirror_agent and name == mirror_agent
        summary = _last_practice.get("summary") if is_mirror else None
        wa: Optional[WebTradingAccount] = _last_practice.get("web_account") if is_mirror else None

        # ── Mirror agent: use real TopstepX data ──
        if is_mirror and (summary or wa):
            # Positions + balance from trading client summary
            if summary:
                positions = summary.get("positions", [])
                balance = summary.get("balance", 0)
                equity = summary.get("equity", 0)
                can_trade = summary.get("canTrade", True)
                account_id = summary.get("accountId")
            else:
                positions = []
                balance = wa.balance if wa else 0
                equity = wa.balance if wa else 0
                can_trade = True
                account_id = None

            # Recalculate unrealized P&L with latest tick prices
            prices = dict(TopstepXAccountClient._current_prices)
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

            snapshot = {
                "accountId": account_id,
                "name": name,
                "balance": wa.balance if wa else balance,
                "equity": wa.balance + sum(p.get("unrealizedPnL", 0) for p in positions) if wa else equity,
                "canTrade": can_trade,
                "positions": positions,
            }

            if wa:
                snapshot["webAccount"] = {
                    "winRate": wa.win_rate,
                    "totalTrades": wa.total_trades,
                    "totalProfit": wa.total_profit,
                    "totalLoss": wa.total_loss,
                    "maximumLoss": wa.maximum_loss,
                    "highestBalance": wa.highest_balance,
                    "startOfDayBalance": wa.start_of_day_balance,
                    "realizedDayPnl": wa.realized_day_pnl,
                    "dailyLoss": wa.daily_loss,
                    "profitAndLoss": wa.profit_and_loss,
                    "startingBalance": wa.starting_balance,
                }
                snapshot["practiceAccount"] = {
                    "status": wa.status,
                    "ineligible": wa.ineligible,
                    "blown": wa.status == 6 or wa.ineligible,
                }
            else:
                snapshot["webAccount"] = {
                    "winRate": 0, "totalTrades": 0, "totalProfit": 0, "totalLoss": 0,
                    "maximumLoss": 4500, "highestBalance": balance,
                    "startOfDayBalance": balance, "realizedDayPnl": 0,
                    "dailyLoss": 0, "profitAndLoss": 0, "startingBalance": 150000,
                }

            return snapshot

        # ── Non-mirror agents: use sim account data ──
        portfolio = _agent_snapshots.get(name)
        if portfolio is None:
            return {"error": f"Loading {name}..."}

        prices = dict(TopstepXAccountClient._current_prices)
        positions = [dict(p) for p in portfolio.get("positions", [])]

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
        balance = portfolio.get("balance", 0)

        total_trades = portfolio.get("totalTrades", 0)
        snapshot = {
            "accountId": portfolio.get("accountId"),
            "name": name,
            "balance": balance,
            "equity": balance + total_unrealized,
            "canTrade": portfolio.get("canTrade", True),
            "positions": positions,
            "webAccount": {
                "winRate": (portfolio["winningTrades"] / total_trades) if total_trades > 0 else 0,
                "totalTrades": total_trades,
                "totalProfit": portfolio.get("totalProfit", 0),
                "totalLoss": portfolio.get("totalLoss", 0),
                "maximumLoss": portfolio.get("drawdownLimit", 4500),
                "highestBalance": portfolio.get("highestBalance", balance),
                "startOfDayBalance": portfolio.get("startOfDayBalance", balance),
                "realizedDayPnl": portfolio.get("realizedDayPnl", 0),
                "dailyLoss": min(0, portfolio.get("realizedDayPnl", 0)),
                "profitAndLoss": portfolio.get("totalRealizedPnl", 0),
                "startingBalance": portfolio.get("startingBalance", 150000),
            },
            "simAccount": {
                "mllFloor": portfolio.get("mllFloor", 145500),
                "blown": portfolio.get("blown", False),
                "drawdownLimit": portfolio.get("drawdownLimit", 4500),
                "dailyTrades": portfolio.get("dailyTrades", 0),
            },
        }

        return snapshot

    def _build_ws_data() -> dict:
        """Build the full WebSocket payload with per-agent data."""
        from unified_market_connector import UnifiedMarketConnector

        prices = dict(TopstepXAccountClient._current_prices)
        market_open = UnifiedMarketConnector._is_market_open()
        agent_states = get_all_agents_state() if get_all_agents_state else {}

        agents_data = {}
        for name in _get_agent_names():
            snapshot = _build_agent_snapshot(name)
            if "error" in snapshot:
                snapshot = {"name": name, "error": snapshot["error"]}
            # Merge activity state (model, logo, sentiment, reasoning, activity)
            state = agent_states.get(name, {})
            snapshot.update({
                "agent_name": name,
                "model": state.get("model", ""),
                "logo": state.get("logo", ""),
                "strategy": state.get("strategy", ""),
                "sentiment": state.get("sentiment", "neutral"),
                "last_active": state.get("last_active"),
                "latest_reasoning": state.get("latest_reasoning"),
                "activity": state.get("activity", []),
            })
            agents_data[name] = snapshot

        return {
            "prices": prices,
            "market_open": market_open,
            "mirror_agent": mirror_agent,
            "agents": agents_data,
        }

    # ── Routes ────────────────────────────────────────────────────

    @app.get("/")
    async def serve_dashboard():
        from starlette.responses import HTMLResponse

        html = (STATIC_DIR / "index.html").read_text()
        if view_only:
            import re
            html = re.sub(
                r'<div id="tradeSection">.*?</div>\s*</div>\s*</div>',
                '',
                html,
                flags=re.DOTALL,
            )
        return HTMLResponse(html)

    @app.get("/api/config")
    async def api_config():
        return {"viewOnly": view_only}

    @app.get("/api/portfolio")
    async def api_portfolio(agent: str = Query(default="")):
        target = agent if agent else agent_name
        return _build_agent_snapshot(target)

    @app.get("/api/prices")
    async def api_prices():
        return dict(TopstepXAccountClient._current_prices)

    @app.get("/api/account-stats")
    async def api_account_stats(agent: str = Query(default="")):
        target = agent if agent else agent_name
        snapshot = _build_agent_snapshot(target)
        if "error" in snapshot:
            return snapshot
        return snapshot.get("webAccount", {})

    @app.get("/api/balance-history")
    async def api_balance_history(timeRange: str = Query(...), agent: str = Query(default="")):
        """Fetch daily balance + MLL history.

        Tries Topstep dashboard API first for mirror agent; falls back to sim_daily_snapshots.
        """
        target = agent if agent else agent_name
        is_mirror = mirror_agent and target == mirror_agent
        if is_mirror and dashboard_client and get_account_id:
            if _dash_account_id["value"] is None:
                topstepx_id = get_account_id()
                if topstepx_id is not None:
                    found = await dashboard_client.find_dashboard_account_id(topstepx_id)
                    if found:
                        _dash_account_id["value"] = found
                        logger.info(f"Mapped TopstepX account {topstepx_id} -> dashboard account {found}")

            if _dash_account_id["value"] is not None:
                try:
                    stats = await dashboard_client.get_account_stats(
                        _dash_account_id["value"], timeRange
                    )
                    return {
                        "balanceHistory": [
                            {"tradeDay": e.trade_day, "balance": e.balance, "dailyProfit": e.daily_profit}
                            for e in stats.balance_history
                        ],
                        "mllHistory": [
                            {"tradeDay": e.trade_day, "maxLossLimit": e.max_loss_limit}
                            for e in stats.mll_history
                        ],
                        "startingBalance": stats.starting_balance,
                        "currentMaxLossLimit": stats.current_max_loss_limit,
                        "targetBalance": stats.target_balance,
                        "performance": {
                            "winRate": stats.performance.win_rate,
                            "profitFactor": stats.performance.profit_factor,
                            "sharpeRatio": stats.performance.sharpe_ratio,
                            "averageWin": stats.performance.average_win,
                            "averageLoss": stats.performance.average_loss,
                        } if stats.performance else None,
                    }
                except Exception as e:
                    logger.error(f"Dashboard balance history error: {e}")

        # Fallback: sim daily snapshots
        history = await sim_manager.get_balance_history(target, days=90)
        portfolio = _agent_snapshots.get(target, {})
        starting_balance = portfolio.get("startingBalance", 150000) if portfolio else 150000

        return {
            "balanceHistory": [
                {"tradeDay": e["tradeDay"], "balance": e["balance"], "dailyProfit": e["dailyProfit"]}
                for e in history
            ],
            "mllHistory": [
                {"tradeDay": e["tradeDay"], "maxLossLimit": e["mllFloor"]}
                for e in history
            ],
            "startingBalance": starting_balance,
        }

    @app.get("/api/leaderboard")
    async def api_leaderboard():
        """Multi-agent arena leaderboard."""
        return await sim_manager.get_all_accounts_summary()

    @app.get("/api/agents")
    async def api_agents():
        if get_all_agents_state:
            return get_all_agents_state()
        return {}

    @app.post("/api/buy")
    async def api_buy(
        contract: str = Query(...),
        quantity: int = Query(..., gt=0),
        agent: str = Query(default=""),
    ):
        if view_only:
            return JSONResponse({"success": False, "error": "Dashboard is in view-only mode"}, 403)
        target = agent if agent else agent_name
        result = await sim_manager.execute_buy(target, contract, quantity)
        return result

    @app.post("/api/sell")
    async def api_sell(
        contract: str = Query(...),
        quantity: int = Query(..., gt=0),
        agent: str = Query(default=""),
    ):
        if view_only:
            return JSONResponse({"success": False, "error": "Dashboard is in view-only mode"}, 403)
        target = agent if agent else agent_name
        result = await sim_manager.execute_sell(target, contract, quantity)
        return result

    @app.post("/api/close")
    async def api_close(contract: str = Query(...), agent: str = Query(default="")):
        if view_only:
            return JSONResponse({"success": False, "error": "Dashboard is in view-only mode"}, 403)
        target = agent if agent else agent_name
        result = await sim_manager.execute_close(target, contract)
        return result

    @app.post("/api/reset-account")
    async def api_reset_account(agent: str = Query(default="")):
        """Reset a sim account back to starting balance. Requires confirmation."""
        if view_only:
            return JSONResponse({"success": False, "error": "Dashboard is in view-only mode"}, 403)
        target = agent if agent else agent_name
        result = await sim_manager.reset_account(target)
        return result

    # ── WebSocket ─────────────────────────────────────────────────

    @app.websocket("/ws")
    async def ws_portfolio(ws: WebSocket):
        await ws.accept()
        logger.info("WebSocket client connected")
        try:
            while True:
                data = _build_ws_data()
                await ws.send_json(data)
                await asyncio.sleep(1)
        except WebSocketDisconnect:
            logger.info("WebSocket client disconnected")
        except Exception as e:
            logger.error(f"WebSocket error: {e}", exc_info=True)

    return app
