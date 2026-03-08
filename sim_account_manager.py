"""
Simulated Trading Account Manager — MySQL-backed arena accounts.

Each agent gets its own persistent account with realistic P&L calculation
driven by live market prices from the TopstepX RTC feed / Kafka.
Only order execution is simulated; prices are real.

Defaults: 150K starting balance, $4,500 trailing MLL.
"""

import asyncio
import logging
import time as _time
from datetime import date, datetime, time, timezone
from typing import Optional

import aiomysql

from topstepx_account import TopstepXAccountClient

logger = logging.getLogger(__name__)

# ── Default account parameters ───────────────────────────────────
DEFAULT_STARTING_BALANCE = 150_000.0
DEFAULT_DRAWDOWN_LIMIT = 4_500.0

# ── DDL ──────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS sim_accounts (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    agent_name      VARCHAR(128) NOT NULL UNIQUE,
    starting_balance DOUBLE NOT NULL DEFAULT 150000,
    balance         DOUBLE NOT NULL DEFAULT 150000,
    start_of_day_balance DOUBLE NOT NULL DEFAULT 150000,
    realized_day_pnl DOUBLE NOT NULL DEFAULT 0,
    total_realized_pnl DOUBLE NOT NULL DEFAULT 0,
    total_profit    DOUBLE NOT NULL DEFAULT 0,
    total_loss      DOUBLE NOT NULL DEFAULT 0,
    highest_balance DOUBLE NOT NULL DEFAULT 150000,
    highest_unrealized_balance DOUBLE NOT NULL DEFAULT 150000,
    highest_realized_balance DOUBLE NOT NULL DEFAULT 150000,
    drawdown_limit  DOUBLE NOT NULL DEFAULT 4500,
    mll_floor       DOUBLE NOT NULL DEFAULT 145500,
    total_trades    INT NOT NULL DEFAULT 0,
    daily_trades    INT NOT NULL DEFAULT 0,
    winning_trades  INT NOT NULL DEFAULT 0,
    losing_trades   INT NOT NULL DEFAULT 0,
    can_trade       TINYINT(1) NOT NULL DEFAULT 1,
    blown           TINYINT(1) NOT NULL DEFAULT 0,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sim_positions (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    account_id  BIGINT NOT NULL,
    symbol      VARCHAR(64) NOT NULL,
    quantity    INT NOT NULL,
    avg_price   DOUBLE NOT NULL,
    tick_size   DOUBLE NOT NULL,
    tick_value  DOUBLE NOT NULL,
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_account_symbol (account_id, symbol),
    FOREIGN KEY (account_id) REFERENCES sim_accounts(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sim_trades (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    account_id    BIGINT NOT NULL,
    symbol        VARCHAR(64) NOT NULL,
    side          VARCHAR(8) NOT NULL,
    quantity      INT NOT NULL,
    entry_price   DOUBLE NOT NULL,
    exit_price    DOUBLE NOT NULL,
    realized_pnl  DOUBLE NOT NULL,
    is_win        TINYINT(1) NOT NULL,
    tick_size     DOUBLE NOT NULL,
    tick_value    DOUBLE NOT NULL,
    opened_at     DATETIME,
    closed_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    duration_secs INT NOT NULL DEFAULT 0,
    FOREIGN KEY (account_id) REFERENCES sim_accounts(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sim_orders (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    account_id    BIGINT NOT NULL,
    symbol        VARCHAR(64) NOT NULL,
    side          VARCHAR(8) NOT NULL,
    quantity      INT NOT NULL,
    fill_price    DOUBLE NOT NULL DEFAULT 0,
    status        VARCHAR(16) NOT NULL DEFAULT 'FILLED',
    reject_reason VARCHAR(256),
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (account_id) REFERENCES sim_accounts(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sim_daily_snapshots (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    account_id      BIGINT NOT NULL,
    trade_date      DATE NOT NULL,
    balance         DOUBLE NOT NULL,
    equity          DOUBLE NOT NULL,
    realized_pnl    DOUBLE NOT NULL DEFAULT 0,
    cumulative_pnl  DOUBLE NOT NULL DEFAULT 0,
    mll_floor       DOUBLE NOT NULL,
    trade_count     INT NOT NULL DEFAULT 0,
    win_count       INT NOT NULL DEFAULT 0,
    UNIQUE KEY uq_account_date (account_id, trade_date),
    FOREIGN KEY (account_id) REFERENCES sim_accounts(id)
) ENGINE=InnoDB;
"""


class SimAccountManager:
    """Async MySQL manager for simulated trading accounts."""

    def __init__(self):
        self._pool: Optional[aiomysql.Pool] = None

    # ── Lifecycle ────────────────────────────────────────────────

    async def initialize(
        self,
        host: str = "localhost",
        port: int = 3306,
        user: str = "trading",
        password: str = "trading_pass",
        db: str = "trading_arena",
    ) -> None:
        self._pool = await aiomysql.create_pool(
            host=host, port=port, user=user, password=password,
            db=db, minsize=2, maxsize=10, autocommit=False,
        )
        # Run DDL
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                for stmt in _DDL.strip().split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        await cur.execute(stmt)
            await conn.commit()
        logger.info("SimAccountManager initialized — schema ready")

    async def close(self) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()

    # ── Account management ───────────────────────────────────────

    async def get_or_create_account(
        self,
        agent_name: str,
        starting_balance: float = DEFAULT_STARTING_BALANCE,
        drawdown_limit: float = DEFAULT_DRAWDOWN_LIMIT,
    ) -> dict:
        mll_floor = starting_balance - drawdown_limit
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    """INSERT INTO sim_accounts
                       (agent_name, starting_balance, balance, start_of_day_balance,
                        highest_balance, highest_unrealized_balance, highest_realized_balance,
                        drawdown_limit, mll_floor)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE updated_at=NOW()""",
                    (agent_name, starting_balance, starting_balance, starting_balance,
                     starting_balance, starting_balance, starting_balance,
                     drawdown_limit, mll_floor),
                )
                await conn.commit()
                await cur.execute(
                    "SELECT * FROM sim_accounts WHERE agent_name=%s", (agent_name,)
                )
                return await cur.fetchone()

    async def _get_account_row(self, cur, agent_name: str, for_update: bool = False) -> Optional[dict]:
        sql = "SELECT * FROM sim_accounts WHERE agent_name=%s"
        if for_update:
            sql += " FOR UPDATE"
        await cur.execute(sql, (agent_name,))
        return await cur.fetchone()

    # ── Price helpers ────────────────────────────────────────────

    @staticmethod
    def _get_live_price(symbol: str) -> Optional[float]:
        return TopstepXAccountClient.get_market_price(symbol)

    @staticmethod
    def _get_specs(symbol: str) -> Optional[dict]:
        return TopstepXAccountClient.get_contract_specs(symbol)

    # ── Order logging ────────────────────────────────────────────

    async def _log_order(self, cur, account_id: int, symbol: str, side: str,
                         quantity: int, fill_price: float, status: str = "FILLED",
                         reject_reason: str = None) -> None:
        await cur.execute(
            """INSERT INTO sim_orders
               (account_id, symbol, side, quantity, fill_price, status, reject_reason)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (account_id, symbol, side, quantity, fill_price, status, reject_reason),
        )

    # ── Execute BUY ──────────────────────────────────────────────

    async def execute_buy(self, agent_name: str, symbol: str, quantity: int) -> dict:
        price = self._get_live_price(symbol)
        if price is None:
            return {"success": False, "error": f"No live price for {symbol} — market may be closed or data not yet streaming"}

        specs = self._get_specs(symbol)
        if specs is None:
            return {"success": False, "error": f"No contract specs for {symbol}"}

        tick_size = specs["tickSize"]
        tick_value = specs["tickValue"]

        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                acct = await self._get_account_row(cur, agent_name, for_update=True)
                if acct is None:
                    await conn.rollback()
                    return {"success": False, "error": f"Account '{agent_name}' not found"}

                if not acct["can_trade"]:
                    await self._log_order(cur, acct["id"], symbol, "BUY", quantity, 0, "REJECTED", "Account blown or cannot trade")
                    await conn.commit()
                    return {"success": False, "error": "Account cannot trade (MLL breached)"}

                # Get existing positions for this account
                await cur.execute(
                    "SELECT * FROM sim_positions WHERE account_id=%s", (acct["id"],)
                )
                positions = await cur.fetchall()

                # Hedging guard: reject buy if any open short
                for pos in positions:
                    if pos["quantity"] < 0:
                        reason = f"SHORT position exists ({abs(pos['quantity'])}x {pos['symbol']})"
                        await self._log_order(cur, acct["id"], symbol, "BUY", quantity, 0, "REJECTED", reason)
                        await conn.commit()
                        return {"success": False, "error": f"BLOCKED: {reason}. Close short first."}

                # No adding to losers
                for pos in positions:
                    if pos["symbol"] == symbol and pos["quantity"] > 0:
                        live = self._get_live_price(pos["symbol"])
                        if live is not None:
                            unrealized = ((live - pos["avg_price"]) / pos["tick_size"]) * pos["tick_value"] * pos["quantity"]
                            if unrealized < 0:
                                reason = f"LONG {pos['quantity']}x {pos['symbol']} is losing (P&L: ${unrealized:+,.2f})"
                                await self._log_order(cur, acct["id"], symbol, "BUY", quantity, 0, "REJECTED", reason)
                                await conn.commit()
                                return {"success": False, "error": f"BLOCKED: {reason}. Cut the loss first."}

                # Fill the order at current price
                existing = None
                for pos in positions:
                    if pos["symbol"] == symbol:
                        existing = pos
                        break

                if existing and existing["quantity"] > 0:
                    # Average into existing long
                    old_qty = existing["quantity"]
                    new_qty = old_qty + quantity
                    new_avg = (existing["avg_price"] * old_qty + price * quantity) / new_qty
                    await cur.execute(
                        "UPDATE sim_positions SET quantity=%s, avg_price=%s WHERE id=%s",
                        (new_qty, new_avg, existing["id"]),
                    )
                else:
                    # New position
                    await cur.execute(
                        """INSERT INTO sim_positions
                           (account_id, symbol, quantity, avg_price, tick_size, tick_value)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        (acct["id"], symbol, quantity, price, tick_size, tick_value),
                    )

                await self._log_order(cur, acct["id"], symbol, "BUY", quantity, price)
                await conn.commit()

        logger.info(f"SIM BUY: {agent_name} bought {quantity}x {symbol} @ ${price:,.2f}")
        return {"success": True, "fill_price": price, "quantity": quantity, "symbol": symbol}

    # ── Execute SELL ─────────────────────────────────────────────

    async def execute_sell(self, agent_name: str, symbol: str, quantity: int) -> dict:
        price = self._get_live_price(symbol)
        if price is None:
            return {"success": False, "error": f"No live price for {symbol} — market may be closed or data not yet streaming"}

        specs = self._get_specs(symbol)
        if specs is None:
            return {"success": False, "error": f"No contract specs for {symbol}"}

        tick_size = specs["tickSize"]
        tick_value = specs["tickValue"]

        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                acct = await self._get_account_row(cur, agent_name, for_update=True)
                if acct is None:
                    await conn.rollback()
                    return {"success": False, "error": f"Account '{agent_name}' not found"}

                if not acct["can_trade"]:
                    await self._log_order(cur, acct["id"], symbol, "SELL", quantity, 0, "REJECTED", "Account blown or cannot trade")
                    await conn.commit()
                    return {"success": False, "error": "Account cannot trade (MLL breached)"}

                await cur.execute(
                    "SELECT * FROM sim_positions WHERE account_id=%s", (acct["id"],)
                )
                positions = await cur.fetchall()

                # Hedging guard: reject sell if any open long
                for pos in positions:
                    if pos["quantity"] > 0:
                        reason = f"LONG position exists ({pos['quantity']}x {pos['symbol']})"
                        await self._log_order(cur, acct["id"], symbol, "SELL", quantity, 0, "REJECTED", reason)
                        await conn.commit()
                        return {"success": False, "error": f"BLOCKED: {reason}. Close long first."}

                # No adding to losers
                for pos in positions:
                    if pos["symbol"] == symbol and pos["quantity"] < 0:
                        live = self._get_live_price(pos["symbol"])
                        if live is not None:
                            unrealized = ((live - pos["avg_price"]) / pos["tick_size"]) * pos["tick_value"] * pos["quantity"]
                            if unrealized < 0:
                                reason = f"SHORT {abs(pos['quantity'])}x {pos['symbol']} is losing (P&L: ${unrealized:+,.2f})"
                                await self._log_order(cur, acct["id"], symbol, "SELL", quantity, 0, "REJECTED", reason)
                                await conn.commit()
                                return {"success": False, "error": f"BLOCKED: {reason}. Cut the loss first."}

                existing = None
                for pos in positions:
                    if pos["symbol"] == symbol:
                        existing = pos
                        break

                if existing and existing["quantity"] < 0:
                    # Average into existing short
                    old_qty = abs(existing["quantity"])
                    new_qty = old_qty + quantity
                    new_avg = (existing["avg_price"] * old_qty + price * quantity) / new_qty
                    await cur.execute(
                        "UPDATE sim_positions SET quantity=%s, avg_price=%s WHERE id=%s",
                        (-new_qty, new_avg, existing["id"]),
                    )
                else:
                    # New short position
                    await cur.execute(
                        """INSERT INTO sim_positions
                           (account_id, symbol, quantity, avg_price, tick_size, tick_value)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        (acct["id"], symbol, -quantity, price, tick_size, tick_value),
                    )

                await self._log_order(cur, acct["id"], symbol, "SELL", quantity, price)
                await conn.commit()

        logger.info(f"SIM SELL: {agent_name} sold {quantity}x {symbol} @ ${price:,.2f}")
        return {"success": True, "fill_price": price, "quantity": quantity, "symbol": symbol}

    # ── Execute CLOSE ────────────────────────────────────────────

    async def execute_close(self, agent_name: str, symbol: str, quantity: int = 0) -> dict:
        price = self._get_live_price(symbol)
        if price is None:
            return {"success": False, "error": f"No live price for {symbol}"}

        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                acct = await self._get_account_row(cur, agent_name, for_update=True)
                if acct is None:
                    await conn.rollback()
                    return {"success": False, "error": f"Account '{agent_name}' not found"}

                await cur.execute(
                    "SELECT * FROM sim_positions WHERE account_id=%s AND symbol=%s FOR UPDATE",
                    (acct["id"], symbol),
                )
                pos = await cur.fetchone()

                if pos is None:
                    # List what they do hold
                    await cur.execute(
                        "SELECT symbol, quantity FROM sim_positions WHERE account_id=%s",
                        (acct["id"],),
                    )
                    held = await cur.fetchall()
                    if not held:
                        await conn.rollback()
                        return {"success": False, "error": "You have ZERO open positions. Nothing to close."}
                    held_str = ", ".join(f"{h['symbol']} qty={h['quantity']}" for h in held)
                    await conn.rollback()
                    return {"success": False, "error": f"You do NOT hold {symbol}. You hold: {held_str}"}

                pos_size = abs(pos["quantity"])
                close_qty = quantity if 0 < quantity < pos_size else pos_size
                is_long = pos["quantity"] > 0
                signed_close = close_qty if is_long else -close_qty

                # P&L calculation
                tick_size = pos["tick_size"]
                tick_value = pos["tick_value"]
                realized_pnl = ((price - pos["avg_price"]) / tick_size) * tick_value * signed_close

                # Update account balances
                new_balance = acct["balance"] + realized_pnl
                new_total_realized = acct["total_realized_pnl"] + realized_pnl
                new_day_pnl = acct["realized_day_pnl"] + realized_pnl
                is_win = realized_pnl > 0

                new_total_profit = acct["total_profit"] + (realized_pnl if realized_pnl > 0 else 0)
                new_total_loss = acct["total_loss"] + (realized_pnl if realized_pnl < 0 else 0)
                new_total_trades = acct["total_trades"] + 1
                new_daily_trades = acct["daily_trades"] + 1
                new_winning = acct["winning_trades"] + (1 if is_win else 0)
                new_losing = acct["losing_trades"] + (1 if not is_win and realized_pnl != 0 else 0)

                # High water marks
                new_highest_balance = max(acct["highest_balance"], new_balance)
                new_highest_realized = max(acct["highest_realized_balance"], new_balance)

                # Trailing MLL floor
                new_mll_floor = max(acct["mll_floor"], new_highest_balance - acct["drawdown_limit"])

                # Blown check
                blown = new_balance < new_mll_floor
                can_trade = not blown

                await cur.execute(
                    """UPDATE sim_accounts SET
                       balance=%s, realized_day_pnl=%s, total_realized_pnl=%s,
                       total_profit=%s, total_loss=%s,
                       highest_balance=%s, highest_realized_balance=%s,
                       mll_floor=%s,
                       total_trades=%s, daily_trades=%s, winning_trades=%s, losing_trades=%s,
                       can_trade=%s, blown=%s
                       WHERE id=%s""",
                    (new_balance, new_day_pnl, new_total_realized,
                     new_total_profit, new_total_loss,
                     new_highest_balance, new_highest_realized,
                     new_mll_floor,
                     new_total_trades, new_daily_trades, new_winning, new_losing,
                     can_trade, blown, acct["id"]),
                )

                # Record trade
                side = "LONG" if is_long else "SHORT"
                opened_at = pos["created_at"]
                closed_at = datetime.now()
                duration = int((closed_at - opened_at).total_seconds()) if opened_at else 0

                await cur.execute(
                    """INSERT INTO sim_trades
                       (account_id, symbol, side, quantity, entry_price, exit_price,
                        realized_pnl, is_win, tick_size, tick_value, opened_at, closed_at, duration_secs)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (acct["id"], symbol, side, close_qty, pos["avg_price"], price,
                     realized_pnl, is_win, tick_size, tick_value, opened_at, closed_at, duration),
                )

                # Update/delete position
                if close_qty >= pos_size:
                    await cur.execute("DELETE FROM sim_positions WHERE id=%s", (pos["id"],))
                else:
                    remaining = pos["quantity"] - signed_close if is_long else pos["quantity"] + close_qty
                    await cur.execute(
                        "UPDATE sim_positions SET quantity=%s WHERE id=%s",
                        (remaining, pos["id"]),
                    )

                await self._log_order(cur, acct["id"], symbol, "CLOSE", close_qty, price)
                await conn.commit()

        action = "CLOSED" if close_qty >= pos_size else f"PARTIAL CLOSE ({close_qty}/{pos_size})"
        logger.info(f"SIM {action}: {agent_name} {symbol} @ ${price:,.2f} | PnL: ${realized_pnl:+,.2f}")

        result = {
            "success": True,
            "fill_price": price,
            "quantity_closed": close_qty,
            "realized_pnl": realized_pnl,
            "symbol": symbol,
            "new_balance": new_balance,
        }
        if blown:
            result["blown"] = True
            result["warning"] = f"Account BLOWN — balance ${new_balance:,.2f} < MLL floor ${new_mll_floor:,.2f}"
            logger.warning(f"ACCOUNT BLOWN: {agent_name} | Balance: ${new_balance:,.2f} < MLL: ${new_mll_floor:,.2f}")
        return result

    # ── Portfolio query ──────────────────────────────────────────

    async def get_portfolio(self, agent_name: str) -> dict:
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                acct = await self._get_account_row(cur, agent_name)
                if acct is None:
                    return {"error": f"Account '{agent_name}' not found"}

                await cur.execute(
                    "SELECT * FROM sim_positions WHERE account_id=%s", (acct["id"],)
                )
                positions = await cur.fetchall()

                pos_list = []
                total_unrealized = 0.0
                for pos in positions:
                    live = self._get_live_price(pos["symbol"])
                    unrealized = 0.0
                    if live is not None:
                        unrealized = ((live - pos["avg_price"]) / pos["tick_size"]) * pos["tick_value"] * pos["quantity"]
                    total_unrealized += unrealized
                    pos_list.append({
                        "symbol": pos["symbol"],
                        "quantity": pos["quantity"],
                        "avgPrice": pos["avg_price"],
                        "marketValue": abs(pos["quantity"]) * (live or pos["avg_price"]),
                        "unrealizedPnL": unrealized,
                    })

                balance = acct["balance"]
                equity = balance + total_unrealized

                # Update highest_unrealized_balance for trailing MLL
                if equity > acct["highest_unrealized_balance"]:
                    await cur.execute(
                        """UPDATE sim_accounts SET
                           highest_unrealized_balance=%s,
                           mll_floor=GREATEST(mll_floor, %s - drawdown_limit)
                           WHERE id=%s""",
                        (equity, equity, acct["id"]),
                    )
                    await conn.commit()

                return {
                    "accountId": acct["id"],
                    "name": acct["agent_name"],
                    "balance": balance,
                    "equity": equity,
                    "canTrade": bool(acct["can_trade"]),
                    "positions": pos_list,
                    "realizedDayPnl": acct["realized_day_pnl"],
                    "totalRealizedPnl": acct["total_realized_pnl"],
                    "totalProfit": acct["total_profit"],
                    "totalLoss": acct["total_loss"],
                    "highestBalance": acct["highest_balance"],
                    "mllFloor": acct["mll_floor"],
                    "drawdownLimit": acct["drawdown_limit"],
                    "startingBalance": acct["starting_balance"],
                    "startOfDayBalance": acct["start_of_day_balance"],
                    "totalTrades": acct["total_trades"],
                    "dailyTrades": acct["daily_trades"],
                    "winningTrades": acct["winning_trades"],
                    "losingTrades": acct["losing_trades"],
                    "blown": bool(acct["blown"]),
                }

    async def get_positions(self, agent_name: str) -> list[dict]:
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                acct = await self._get_account_row(cur, agent_name)
                if acct is None:
                    return []
                await cur.execute(
                    "SELECT * FROM sim_positions WHERE account_id=%s", (acct["id"],)
                )
                rows = await cur.fetchall()
                result = []
                for pos in rows:
                    live = self._get_live_price(pos["symbol"])
                    unrealized = 0.0
                    if live is not None:
                        unrealized = ((live - pos["avg_price"]) / pos["tick_size"]) * pos["tick_value"] * pos["quantity"]
                    result.append({
                        "symbol": pos["symbol"],
                        "quantity": pos["quantity"],
                        "avgPrice": pos["avg_price"],
                        "unrealizedPnL": unrealized,
                    })
                return result

    async def get_win_rate(self, agent_name: str) -> Optional[float]:
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                acct = await self._get_account_row(cur, agent_name)
                if acct is None or acct["total_trades"] == 0:
                    return None
                return acct["winning_trades"] / acct["total_trades"]

    async def get_balance_history(self, agent_name: str, days: int = 30) -> list[dict]:
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                acct = await self._get_account_row(cur, agent_name)
                if acct is None:
                    return []
                await cur.execute(
                    """SELECT trade_date, balance, equity, realized_pnl,
                              cumulative_pnl, mll_floor, trade_count, win_count
                       FROM sim_daily_snapshots
                       WHERE account_id=%s
                       ORDER BY trade_date DESC
                       LIMIT %s""",
                    (acct["id"], days),
                )
                rows = await cur.fetchall()
                return [
                    {
                        "tradeDay": str(r["trade_date"]),
                        "balance": r["balance"],
                        "equity": r["equity"],
                        "dailyProfit": r["realized_pnl"],
                        "cumulativePnl": r["cumulative_pnl"],
                        "mllFloor": r["mll_floor"],
                        "tradeCount": r["trade_count"],
                        "winCount": r["win_count"],
                    }
                    for r in reversed(rows)
                ]

    async def get_all_accounts_summary(self) -> list[dict]:
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT * FROM sim_accounts ORDER BY balance DESC")
                accounts = await cur.fetchall()
                result = []
                for acct in accounts:
                    # Calculate live equity
                    await cur.execute(
                        "SELECT * FROM sim_positions WHERE account_id=%s", (acct["id"],)
                    )
                    positions = await cur.fetchall()
                    total_unrealized = 0.0
                    for pos in positions:
                        live = self._get_live_price(pos["symbol"])
                        if live is not None:
                            total_unrealized += ((live - pos["avg_price"]) / pos["tick_size"]) * pos["tick_value"] * pos["quantity"]

                    win_rate = (acct["winning_trades"] / acct["total_trades"] * 100) if acct["total_trades"] > 0 else 0

                    result.append({
                        "agentName": acct["agent_name"],
                        "balance": acct["balance"],
                        "equity": acct["balance"] + total_unrealized,
                        "totalRealizedPnl": acct["total_realized_pnl"],
                        "unrealizedPnl": total_unrealized,
                        "totalTrades": acct["total_trades"],
                        "winRate": win_rate,
                        "mllFloor": acct["mll_floor"],
                        "canTrade": bool(acct["can_trade"]),
                        "blown": bool(acct["blown"]),
                    })
                return result

    async def reset_account(self, agent_name: str) -> dict:
        """Manually reset an account back to starting state.

        Closes any open positions (without recording P&L), clears all stats,
        and restores the starting balance. The account can trade again.
        Trade history (sim_trades, sim_orders) is preserved for audit.
        """
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                acct = await self._get_account_row(cur, agent_name, for_update=True)
                if acct is None:
                    await conn.rollback()
                    return {"success": False, "error": f"Account '{agent_name}' not found"}

                starting = acct["starting_balance"]
                dd_limit = acct["drawdown_limit"]
                mll_floor = starting - dd_limit

                # Delete all open positions
                await cur.execute(
                    "DELETE FROM sim_positions WHERE account_id=%s", (acct["id"],)
                )

                # Reset account to starting state
                await cur.execute(
                    """UPDATE sim_accounts SET
                       balance=%s, start_of_day_balance=%s,
                       realized_day_pnl=0, total_realized_pnl=0,
                       total_profit=0, total_loss=0,
                       highest_balance=%s, highest_unrealized_balance=%s,
                       highest_realized_balance=%s,
                       mll_floor=%s,
                       total_trades=0, daily_trades=0,
                       winning_trades=0, losing_trades=0,
                       can_trade=1, blown=0
                       WHERE id=%s""",
                    (starting, starting, starting, starting, starting,
                     mll_floor, acct["id"]),
                )
                await conn.commit()

        logger.info(f"Account RESET: {agent_name} -> ${starting:,.2f}")
        return {"success": True, "balance": starting, "agent_name": agent_name}

    async def daily_snapshot(self, agent_name: str) -> None:
        """Write a point-in-time snapshot to sim_daily_snapshots for equity curve tracking.

        Does NOT reset any account fields — all stats are cumulative and permanent.
        """
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                acct = await self._get_account_row(cur, agent_name)
                if acct is None:
                    return

                today = date.today()
                await cur.execute(
                    """INSERT INTO sim_daily_snapshots
                       (account_id, trade_date, balance, equity, realized_pnl,
                        cumulative_pnl, mll_floor, trade_count, win_count)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE
                       balance=%s, equity=%s, realized_pnl=%s, cumulative_pnl=%s,
                       mll_floor=%s, trade_count=%s, win_count=%s""",
                    (acct["id"], today, acct["balance"], acct["balance"],
                     acct["realized_day_pnl"], acct["total_realized_pnl"],
                     acct["mll_floor"], acct["total_trades"], acct["winning_trades"],
                     acct["balance"], acct["balance"], acct["realized_day_pnl"],
                     acct["total_realized_pnl"], acct["mll_floor"],
                     acct["total_trades"], acct["winning_trades"]),
                )
                await conn.commit()
        logger.info(f"Daily snapshot recorded for {agent_name}")

    async def daily_snapshot_all(self) -> None:
        """Write a daily snapshot for every account."""
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT agent_name FROM sim_accounts")
                rows = await cur.fetchall()
        for row in rows:
            await self.daily_snapshot(row["agent_name"])

    # ── Continuous MLL monitor ───────────────────────────────────

    async def check_mll_all_accounts(self) -> list[dict]:
        """Check every account with open positions for MLL breach.

        Updates highest_unrealized_balance / mll_floor (ratchet up) and
        force-liquidates + marks blown when equity < mll_floor.

        Returns list of accounts that were just blown.
        """
        blown_accounts: list[dict] = []
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                # Only check accounts that can still trade
                await cur.execute("SELECT * FROM sim_accounts WHERE can_trade=1")
                accounts = await cur.fetchall()

        for acct in accounts:
            async with self._pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    # Re-lock the row for update
                    await cur.execute(
                        "SELECT * FROM sim_accounts WHERE id=%s FOR UPDATE", (acct["id"],)
                    )
                    acct = await cur.fetchone()
                    if not acct or not acct["can_trade"]:
                        await conn.rollback()
                        continue

                    await cur.execute(
                        "SELECT * FROM sim_positions WHERE account_id=%s", (acct["id"],)
                    )
                    positions = await cur.fetchall()

                    if not positions:
                        await conn.rollback()
                        continue

                    # Calculate live equity
                    total_unrealized = 0.0
                    for pos in positions:
                        live = self._get_live_price(pos["symbol"])
                        if live is not None:
                            total_unrealized += (
                                (live - pos["avg_price"]) / pos["tick_size"]
                            ) * pos["tick_value"] * pos["quantity"]

                    equity = acct["balance"] + total_unrealized

                    # Ratchet up high water mark + MLL floor
                    new_highest = max(acct["highest_unrealized_balance"], equity)
                    new_mll_floor = max(
                        acct["mll_floor"],
                        new_highest - acct["drawdown_limit"],
                    )

                    if equity < new_mll_floor:
                        # ── MLL BREACH: force-close all positions ────
                        total_realized = 0.0
                        for pos in positions:
                            live = self._get_live_price(pos["symbol"])
                            if live is None:
                                live = pos["avg_price"]  # fallback: flat close

                            is_long = pos["quantity"] > 0
                            signed_qty = pos["quantity"]
                            pnl = (
                                (live - pos["avg_price"]) / pos["tick_size"]
                            ) * pos["tick_value"] * signed_qty
                            total_realized += pnl

                            side = "LONG" if is_long else "SHORT"
                            opened_at = pos["created_at"]
                            closed_at = datetime.now()
                            duration = (
                                int((closed_at - opened_at).total_seconds())
                                if opened_at else 0
                            )

                            await cur.execute(
                                """INSERT INTO sim_trades
                                   (account_id, symbol, side, quantity, entry_price,
                                    exit_price, realized_pnl, is_win, tick_size,
                                    tick_value, opened_at, closed_at, duration_secs)
                                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                                (acct["id"], pos["symbol"], side, abs(pos["quantity"]),
                                 pos["avg_price"], live, pnl, pnl > 0,
                                 pos["tick_size"], pos["tick_value"],
                                 opened_at, closed_at, duration),
                            )
                            await self._log_order(
                                cur, acct["id"], pos["symbol"], "CLOSE",
                                abs(pos["quantity"]), live,
                            )

                        # Delete all positions
                        await cur.execute(
                            "DELETE FROM sim_positions WHERE account_id=%s",
                            (acct["id"],),
                        )

                        # Update account
                        new_balance = acct["balance"] + total_realized
                        new_day_pnl = acct["realized_day_pnl"] + total_realized
                        new_total_realized = acct["total_realized_pnl"] + total_realized
                        new_profit = acct["total_profit"] + max(0, total_realized)
                        new_loss = acct["total_loss"] + min(0, total_realized)
                        new_trades = acct["total_trades"] + len(positions)
                        new_daily = acct["daily_trades"] + len(positions)
                        new_winning = acct["winning_trades"] + sum(
                            1 for _ in positions  # counted properly below
                        )
                        # Recount wins/losses properly
                        wins_added = 0
                        losses_added = 0
                        for pos in positions:
                            live = self._get_live_price(pos["symbol"]) or pos["avg_price"]
                            pnl = ((live - pos["avg_price"]) / pos["tick_size"]) * pos["tick_value"] * pos["quantity"]
                            if pnl > 0:
                                wins_added += 1
                            elif pnl < 0:
                                losses_added += 1

                        await cur.execute(
                            """UPDATE sim_accounts SET
                               balance=%s, realized_day_pnl=%s,
                               total_realized_pnl=%s,
                               total_profit=%s, total_loss=%s,
                               highest_balance=GREATEST(highest_balance, %s),
                               highest_unrealized_balance=%s,
                               highest_realized_balance=GREATEST(highest_realized_balance, %s),
                               mll_floor=%s,
                               total_trades=%s, daily_trades=%s,
                               winning_trades=%s, losing_trades=%s,
                               can_trade=0, blown=1
                               WHERE id=%s""",
                            (new_balance, new_day_pnl, new_total_realized,
                             new_profit, new_loss,
                             new_balance, new_highest, new_balance,
                             new_mll_floor,
                             new_trades, new_daily,
                             acct["winning_trades"] + wins_added,
                             acct["losing_trades"] + losses_added,
                             acct["id"]),
                        )
                        await conn.commit()

                        blown_accounts.append({
                            "agent_name": acct["agent_name"],
                            "balance": new_balance,
                            "mll_floor": new_mll_floor,
                            "equity_at_breach": equity,
                        })
                        logger.warning(
                            f"MLL BREACH — FORCE LIQUIDATED: {acct['agent_name']} | "
                            f"Equity ${equity:,.2f} < MLL ${new_mll_floor:,.2f} | "
                            f"New balance: ${new_balance:,.2f}"
                        )
                    else:
                        # No breach — just update high water marks
                        if (
                            new_highest != acct["highest_unrealized_balance"]
                            or new_mll_floor != acct["mll_floor"]
                        ):
                            await cur.execute(
                                """UPDATE sim_accounts SET
                                   highest_unrealized_balance=%s, mll_floor=%s
                                   WHERE id=%s""",
                                (new_highest, new_mll_floor, acct["id"]),
                            )
                            await conn.commit()
                        else:
                            await conn.rollback()

        return blown_accounts

    # ── Background tasks (started by trading-tools main) ─────────

    async def start_background_tasks(self) -> list[asyncio.Task]:
        """Start the MLL monitor and daily snapshot scheduler.

        Returns the task handles so the caller can cancel them on shutdown.
        """
        tasks = [
            asyncio.create_task(self._mll_monitor_loop()),
            asyncio.create_task(self._daily_snapshot_scheduler()),
        ]
        logger.info("Background tasks started: MLL monitor + daily snapshot scheduler")
        return tasks

    async def _mll_monitor_loop(self) -> None:
        """Check all accounts for MLL breach every 5 seconds."""
        while True:
            try:
                blown = await self.check_mll_all_accounts()
                for b in blown:
                    logger.warning(
                        f"MLL MONITOR: {b['agent_name']} blown — "
                        f"balance ${b['balance']:,.2f}"
                    )
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"MLL monitor error: {e}")
            await asyncio.sleep(5)

    async def _daily_snapshot_scheduler(self) -> None:
        """Write a daily snapshot for all accounts at ~17:00 CT (CME settlement).

        This only records a point-in-time row in sim_daily_snapshots for equity
        curve history. No account fields are reset — all stats are cumulative
        and accounts that are blown stay blown permanently.
        """
        SNAPSHOT_HOUR_UTC = 22  # ~5 PM CT (winter)

        last_snapshot_date: Optional[date] = None

        while True:
            try:
                now_utc = datetime.now(timezone.utc)
                today = now_utc.date()

                if last_snapshot_date != today and now_utc.hour >= SNAPSHOT_HOUR_UTC:
                    logger.info("Daily snapshot scheduler: recording snapshots")
                    await self.daily_snapshot_all()
                    last_snapshot_date = today
                    logger.info(f"Daily snapshots recorded for {today}")
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"Daily snapshot scheduler error: {e}")
            await asyncio.sleep(60)
