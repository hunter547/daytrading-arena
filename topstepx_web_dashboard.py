"""
FastAPI web dashboard for the TopstepX trading arena.

Provides REST endpoints and WebSocket for live portfolio monitoring.
Sim accounts are backed by MySQL; the promoted agent's real TopstepX account stats
come from the TopstepX API.

Usage:
    app = create_app(sim_manager, get_all_agents_state, agent_name, ...)
    # Then run with uvicorn in the same event loop
"""

import asyncio
import logging
import os
from pathlib import Path
from typing import Callable, Optional

from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

import yaml

from sim_account_manager import SimAccountManager
from topstepx_account import TopstepXAccountClient
from topstepx_web_client import TopstepDashboardClient, TopstepXWebClient, WebTradingAccount

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


def create_app(
    sim_manager: SimAccountManager,
    get_all_agents_state: Optional[Callable[[], dict]] = None,
    agent_name: str = "futures-trader",
    promoted_agent: str = "",
    trading_client=None,
    get_account_id: Optional[Callable[[], Optional[int]]] = None,
    set_account_id: Optional[Callable[[int], None]] = None,
    web_client: Optional[TopstepXWebClient] = None,
    dashboard_client: Optional[TopstepDashboardClient] = None,
    ninjatrader_copytrader=None,
    copy_agent: str = "",
) -> FastAPI:
    """Create FastAPI app backed by SimAccountManager + optional TopstepX API.

    Args:
        sim_manager: SimAccountManager instance for simulated accounts
        get_all_agents_state: Optional callable returning {agent_name: state_dict, ...}
        agent_name: Default agent name for single-agent queries
        promoted_agent: Agent name whose trades are mirrored to real TopstepX account;
                      only this agent's dashboard view uses TopstepX API data
        trading_client: Optional TopstepXTradingClient for real practice account
        get_account_id: Callable returning the current practice account ID
        set_account_id: Callable to update the practice account ID
        web_client: Optional TopstepXWebClient for richer practice account data
        dashboard_client: Optional TopstepDashboardClient for balance history
        ninjatrader_copytrader: Optional NinjaTraderCopyTrader whose account is
                      shown as a "copied account" sub-panel under the copy agent
        copy_agent: Agent name whose trades are copy-traded to NinjaTrader
    """
    app = FastAPI(title="TopstepX Agent Arena", docs_url=None, redoc_url=None)

    reset_password = os.getenv("RESET_PASSWORD", "")

    # Cached sim portfolio snapshots per agent
    _agent_snapshots: dict[str, dict] = {}
    # Cached real TopstepX account data (only used for promoted_agent)
    _last_practice: dict = {"summary": None, "web_account": None}
    # Cached NinjaTrader copy-account snapshot (only used for copy_agent)
    _last_copy: dict = {"summary": None}
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
                # Seed agent names from DB if we don't have any yet
                if not _agent_snapshots and not (get_all_agents_state and get_all_agents_state()):
                    try:
                        db_names = await sim_manager.get_all_agent_names()
                        for n in db_names:
                            _agent_snapshots.setdefault(n, {})
                    except Exception:
                        pass

                # Refresh sim portfolios for all known agents
                for name in _get_agent_names():
                    try:
                        portfolio = await sim_manager.get_portfolio(name)
                        if "error" not in portfolio:
                            _agent_snapshots[name] = portfolio
                    except Exception as e:
                        logger.debug(f"Portfolio refresh error for {name}: {e}")

                # Refresh real TopstepX account stats (only for promoted agent)
                if promoted_agent and trading_client and get_account_id:
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
                            acct = await web_client.get_account_by_id(account_id)
                            if acct:
                                _last_practice["web_account"] = acct
                        except Exception as e:
                            logger.debug(f"Web client refresh error: {e}")

                # Refresh NinjaTrader copy-account snapshot
                if ninjatrader_copytrader is not None:
                    try:
                        copy_summary = await ninjatrader_copytrader.get_account_summary()
                        if "error" not in copy_summary:
                            _last_copy["summary"] = copy_summary
                        else:
                            logger.debug(f"Copy account refresh: {copy_summary['error']}")
                    except Exception as e:
                        logger.debug(f"Copy account refresh error: {e}")
            except Exception as e:
                logger.error(f"Background refresh error: {e}")
            await asyncio.sleep(5)

    @app.on_event("startup")
    async def _start_refresh():
        asyncio.create_task(_refresh_loop())

    def _build_agent_snapshot(name: str) -> dict:
        """Build portfolio snapshot for a single agent.

        For the promoted agent: uses real TopstepX account data exclusively
        (balance, equity, positions, stats) when available.
        For all other agents: uses sim account data from MySQL.
        """
        is_promoted = promoted_agent and name == promoted_agent
        summary = _last_practice.get("summary") if is_promoted else None
        wa: Optional[WebTradingAccount] = _last_practice.get("web_account") if is_promoted else None

        # ── Promoted agent: use real TopstepX data ──
        if is_promoted and (summary or wa):
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
                    "name": wa.account_name,
                }
            else:
                snapshot["webAccount"] = {
                    "winRate": 0, "totalTrades": 0, "totalProfit": 0, "totalLoss": 0,
                    "maximumLoss": 4500, "highestBalance": balance,
                    "startOfDayBalance": balance, "realizedDayPnl": 0,
                    "dailyLoss": 0, "profitAndLoss": 0, "startingBalance": 150000,
                }

            return snapshot

        # ── Non-promoted agents: use sim account data ──
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
                "totalFees": portfolio.get("totalFees", 0),
                "dailyFees": portfolio.get("dailyFees", 0),
            },
            "simAccount": {
                "mllFloor": portfolio.get("mllFloor", 145500),
                "blown": portfolio.get("blown", False),
                "drawdownLimit": portfolio.get("drawdownLimit", 4500),
                "dailyTrades": portfolio.get("dailyTrades", 0),
            },
        }

        return snapshot

    def _build_copy_snapshot() -> Optional[dict]:
        """Build the NinjaTrader copy-account snapshot for the dashboard.

        Returns None when copy trading isn't configured or no data yet.
        """
        if ninjatrader_copytrader is None:
            return None
        summary = _last_copy.get("summary")
        if not summary:
            return {
                "account": getattr(ninjatrader_copytrader, "account", ""),
                "loading": True,
            }
        return {
            "account": summary.get("account", ""),
            "balance": summary.get("balance", 0),
            "equity": summary.get("equity", 0),
            "unrealizedPnL": summary.get("unrealizedPnL", 0),
            "realizedDayPnl": summary.get("realizedDayPnl", 0),
            "winRate": summary.get("winRate", 0),
            "totalTrades": summary.get("totalTrades", 0),
            "totalProfit": summary.get("totalProfit", 0),
            "totalLoss": summary.get("totalLoss", 0),
            "positions": summary.get("positions", []),
            "connection": summary.get("connection", ""),
            "status": summary.get("status", ""),
        }

    def _build_ws_data() -> dict:
        """Build the full WebSocket payload with per-agent data."""
        from unified_market_connector import _is_market_open

        prices = dict(TopstepXAccountClient._current_prices)
        market_open = _is_market_open()
        agent_states = get_all_agents_state() if get_all_agents_state else {}
        copy_snapshot = _build_copy_snapshot()

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
            # Attach the NinjaTrader copy-account view to the copy agent's panel
            if copy_agent and name == copy_agent and copy_snapshot is not None:
                snapshot["copyAccount"] = copy_snapshot
            agents_data[name] = snapshot

        return {
            "prices": prices,
            "market_open": market_open,
            "promoted_agent": promoted_agent,
            "copy_agent": copy_agent,
            "agents": agents_data,
        }

    # ── Routes ────────────────────────────────────────────────────

    @app.get("/")
    async def serve_dashboard():
        from starlette.responses import HTMLResponse

        html = (STATIC_DIR / "index.html").read_text()
        return HTMLResponse(
            html,
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )

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

        Tries Topstep dashboard API first for promoted agent; falls back to sim_daily_snapshots.
        """
        target = agent if agent else agent_name
        is_promoted = promoted_agent and target == promoted_agent
        if is_promoted and dashboard_client and get_account_id:
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

    @app.get("/api/contracts")
    async def api_contracts():
        """Get all active tradeable contracts."""
        contracts = await sim_manager.get_active_contracts()
        return contracts

    @app.get("/api/copy-account")
    async def api_copy_account():
        """Get the NinjaTrader copy-account snapshot (mirrored from the copy agent)."""
        snapshot = _build_copy_snapshot()
        if snapshot is None:
            return {"enabled": False}
        return {"enabled": True, "copy_agent": copy_agent, **snapshot}

    @app.post("/api/reset-account")
    async def api_reset_account(request: Request, agent: str = Query(default="")):
        """Reset a sim account back to starting balance. Requires password if set."""
        if reset_password:
            try:
                body = await request.json()
                pw = body.get("password", "")
            except Exception:
                pw = ""
            if pw != reset_password:
                return JSONResponse({"success": False, "error": "Invalid password"}, 403)
        target = agent if agent else agent_name
        result = await sim_manager.reset_account(target)
        if result.get("success"):
            try:
                from topstepx_trading_tools import clear_blown_cache
                clear_blown_cache(target)
            except ImportError:
                pass
        return result

    # ── Eval results ───────────────────────────────────────────────

    @app.post("/api/eval-result")
    async def api_record_eval(request: Request):
        """Manually record an eval result (pass or fail) for an agent."""
        body = await request.json()
        if reset_password:
            pw = body.get("password", "")
            if pw != reset_password:
                return JSONResponse({"success": False, "error": "Invalid password"}, 403)
        agent = body.get("agent_name", "")
        passed = body.get("passed", False)
        notes = body.get("notes", "")
        if not agent:
            return JSONResponse({"success": False, "error": "agent_name required"}, 400)
        cfg = sim_manager._agent_configs.get(agent, {})
        model_id = body.get("model_id", cfg.get("model_id", "unknown"))
        strategy = body.get("strategy", cfg.get("strategy", "unknown"))
        result = await sim_manager.record_eval_result(
            agent_name=agent,
            model_id=model_id,
            strategy=strategy,
            passed=passed,
            notes=notes,
        )
        return result

    @app.get("/api/eval-results")
    async def api_get_eval_results(agent: str = Query(default="")):
        """Get eval results, optionally filtered by agent."""
        rows = await sim_manager.get_eval_results(agent or None)
        # Convert datetime objects for JSON serialization
        for row in rows:
            if row.get("recorded_at"):
                row["recorded_at"] = row["recorded_at"].isoformat()
        return rows

    @app.get("/api/strategies")
    async def api_get_strategies():
        """Return available strategy names from deploy_router_node.py."""
        try:
            from deploy_router_node import STRATEGIES
            return list(STRATEGIES.keys())
        except ImportError:
            return ["default", "momentum", "brainrot", "scalper", "futures"]

    @app.post("/api/update-strategy")
    async def api_update_strategy(request: Request):
        """Update an agent's strategy in agents.yml. Requires restart to take effect."""
        body = await request.json()
        if reset_password:
            pw = body.get("password", "")
            if pw != reset_password:
                return JSONResponse({"success": False, "error": "Invalid password"}, 403)
        agent = body.get("agent_name", "")
        new_strategy = body.get("strategy", "")
        if not agent or not new_strategy:
            return JSONResponse({"success": False, "error": "agent_name and strategy required"}, 400)

        agents_yml = Path(__file__).parent / "agents.yml"
        try:
            with open(agents_yml) as f:
                config = yaml.safe_load(f)
            if agent not in config.get("agents", {}):
                return JSONResponse({"success": False, "error": f"Agent '{agent}' not in agents.yml"}, 404)
            old_strategy = config["agents"][agent].get("strategy", "")
            config["agents"][agent]["strategy"] = new_strategy
            with open(agents_yml, "w") as f:
                yaml.dump(config, f, default_flow_style=False, sort_keys=False)
            # Update in-memory state
            cfg = sim_manager._agent_configs.get(agent, {})
            if cfg:
                cfg["strategy"] = new_strategy
            # Update dashboard agent state so header reflects the change
            try:
                from topstepx_trading_tools import _get_agent_state
                state = _get_agent_state(agent)
                state["strategy"] = new_strategy
            except ImportError:
                pass
            logger.info(f"Strategy updated: {agent} {old_strategy} -> {new_strategy}")
            return {"success": True, "old_strategy": old_strategy, "new_strategy": new_strategy}
        except Exception as e:
            logger.exception("Failed to update agents.yml")
            return JSONResponse({"success": False, "error": str(e)}, 500)

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
