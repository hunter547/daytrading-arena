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
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional

import aiomysql
import httpx

from topstepx_account import TopstepXAccountClient

logger = logging.getLogger(__name__)

# ── Default account parameters ───────────────────────────────────
DEFAULT_STARTING_BALANCE = 150_000.0
DEFAULT_DRAWDOWN_LIMIT = 4_500.0

# ── Contract group mapping ────────────────────────────────────────
# Maps symbol_id (from API) to (contract_group, is_micro, micro_equivalent)
# symbol_id is the middle portion of contract_id, e.g. "F.US.MES" from "CON.F.US.MES.H26"
CONTRACT_GROUP_MAP: dict[str, tuple[str, bool, int]] = {
    # E-mini S&P 500 family
    "F.US.EP":   ("ES", False, 10),   # E-mini S&P 500
    "F.US.MES":  ("ES", True,  1),    # Micro E-mini S&P 500
    # E-mini NASDAQ family
    "F.US.ENQ":  ("NQ", False, 10),   # E-mini NASDAQ 100
    "F.US.MNQ":  ("NQ", True,  1),    # Micro E-mini NASDAQ 100
    # E-mini Russell family
    "F.US.RTY":  ("RTY", False, 10),  # E-mini Russell 2000
    "F.US.M2K":  ("RTY", True,  1),   # Micro E-mini Russell 2000
    # Crude Oil family
    "F.US.CL":   ("CL", False, 10),   # Crude Oil
    "F.US.QM":   ("CL", False, 5),    # E-mini Crude Oil
    "F.US.MCL":  ("CL", True,  1),    # Micro Crude Oil
    # Gold family
    "F.US.GC":   ("GC", False, 10),   # Gold
    "F.US.MGC":  ("GC", True,  1),    # Micro Gold
    # Other metals
    "F.US.SI":   ("SI", False, 10),   # Silver
    "F.US.HG":   ("HG", False, 10),   # Copper
    # Bitcoin / Ether
    "F.US.MBT":  ("BTC", True,  1),   # Micro Bitcoin
    "F.US.MET":  ("ETH", True,  1),   # Micro Ether
    # Nikkei
    "F.US.NKD":  ("NKD", False, 10),  # Nikkei 225
    # Grains
    "F.US.ZC":   ("ZC", False, 10),   # Corn
    "F.US.ZW":   ("ZW", False, 10),   # Wheat
    "F.US.ZS":   ("ZS", False, 10),   # Soybeans
    # Bonds
    "F.US.ZN":   ("ZN", False, 10),   # 10-Year Note
    "F.US.ZB":   ("ZB", False, 10),   # 30-Year Bond
}

# ── Default commissions by symbol_id (fallback when DB has no data) ──
# Source: https://intercom.help/topstep-llc/en/articles/8284213
# Round-trip (entry + exit combined).
DEFAULT_COMMISSIONS: dict[str, float] = {
    "F.US.ENQ": 2.80,   # E-mini NASDAQ 100
    "F.US.EP":  2.80,   # E-mini S&P 500
    "F.US.RTY": 2.80,   # E-mini Russell 2000
    "F.US.MES": 0.74,   # Micro E-mini S&P
    "F.US.MNQ": 0.74,   # Micro E-mini NASDAQ
    "F.US.M2K": 0.74,   # Micro E-mini Russell 2000
    "F.US.NKD": 4.34,   # Nikkei
    "F.US.MBT": 2.34,   # Micro Bitcoin
    "F.US.MET": 0.24,   # Micro Ether
    "F.US.CL":  3.04,   # Crude Oil
    "F.US.MCL": 1.04,   # Micro Crude Oil
    "F.US.QM":  2.44,   # E-mini Crude Oil
    "F.US.GC":  3.24,   # Gold
    "F.US.MGC": 1.24,   # Micro Gold
    "F.US.SI":  3.24,   # Silver
    "F.US.HG":  3.24,   # Copper
    "F.US.ZC":  4.30,   # Corn
    "F.US.ZW":  4.30,   # Wheat
    "F.US.ZS":  4.30,   # Soybeans
    "F.US.ZN":  1.60,   # 10-Year Note
    "F.US.ZB":  1.78,   # 30-Year Bond
}
DEFAULT_COMMISSION = 0.74  # fallback: MES rate

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
    total_fees      DOUBLE NOT NULL DEFAULT 0,
    daily_fees      DOUBLE NOT NULL DEFAULT 0,
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
    entry_fee     DOUBLE NOT NULL DEFAULT 0,
    exit_fee      DOUBLE NOT NULL DEFAULT 0,
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
    fee           DOUBLE NOT NULL DEFAULT 0,
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

CREATE TABLE IF NOT EXISTS sim_contracts (
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    contract_id      VARCHAR(64) NOT NULL UNIQUE,
    symbol_id        VARCHAR(32) NOT NULL,
    name             VARCHAR(64) NOT NULL,
    description      VARCHAR(256),
    tick_size        DOUBLE NOT NULL,
    tick_value       DOUBLE NOT NULL,
    commission_rt    DOUBLE NOT NULL DEFAULT 0.74,
    is_micro         TINYINT(1) NOT NULL DEFAULT 1,
    micro_equivalent INT NOT NULL DEFAULT 1,
    contract_group   VARCHAR(32) NOT NULL,
    is_active        TINYINT(1) NOT NULL DEFAULT 1,
    is_tradeable     TINYINT(1) NOT NULL DEFAULT 1,
    last_synced_at   DATETIME,
    created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_group (contract_group),
    INDEX idx_symbol (symbol_id),
    INDEX idx_active (is_active, is_tradeable)
) ENGINE=InnoDB;
"""


class SimAccountManager:
    """Async MySQL manager for simulated trading accounts."""

    def __init__(self):
        self._pool: Optional[aiomysql.Pool] = None
        # In-memory cache for contract info (populated from DB)
        self._contract_cache: dict[str, dict] = {}  # contract_id -> row dict
        self._contract_cache_ts: float = 0.0
        self._contract_cache_ttl: float = 300.0  # 5 min

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

        # Migrate existing tables: add fee columns if missing
        await self._migrate_fee_columns()
        logger.info("SimAccountManager initialized — schema ready")

    async def _migrate_fee_columns(self) -> None:
        """Add fee columns to existing tables (safe to run repeatedly)."""
        migrations = [
            ("sim_accounts", "total_fees", "DOUBLE NOT NULL DEFAULT 0"),
            ("sim_accounts", "daily_fees", "DOUBLE NOT NULL DEFAULT 0"),
            ("sim_trades", "entry_fee", "DOUBLE NOT NULL DEFAULT 0"),
            ("sim_trades", "exit_fee", "DOUBLE NOT NULL DEFAULT 0"),
            ("sim_orders", "fee", "DOUBLE NOT NULL DEFAULT 0"),
        ]
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                for table, column, col_def in migrations:
                    await cur.execute(
                        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
                        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s AND COLUMN_NAME=%s",
                        (table, column),
                    )
                    (exists,) = await cur.fetchone()
                    if not exists:
                        await cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_def}")
                        logger.info(f"Migration: added {table}.{column}")
            await conn.commit()

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

    async def get_all_agent_names(self) -> list[str]:
        """Return all agent names from sim_accounts."""
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT agent_name FROM sim_accounts ORDER BY agent_name")
                rows = await cur.fetchall()
                return [r["agent_name"] for r in rows]

    async def get_all_open_position_symbols(self) -> list[str]:
        """Return distinct contract symbols with open positions across all agents."""
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT DISTINCT p.symbol FROM sim_positions p "
                    "JOIN sim_accounts a ON p.account_id = a.id "
                    "WHERE p.quantity != 0"
                )
                rows = await cur.fetchall()
                return [r["symbol"] for r in rows]

    # ── Price helpers ────────────────────────────────────────────

    @staticmethod
    def _get_live_price(symbol: str) -> Optional[float]:
        return TopstepXAccountClient.get_market_price(symbol)

    @staticmethod
    def _get_specs(symbol: str) -> Optional[dict]:
        return TopstepXAccountClient.get_contract_specs(symbol)

    # ── Commission helpers ────────────────────────────────────────

    async def _get_commission(self, symbol: str) -> float:
        """Get round-trip commission+fees for a contract, checking DB first."""
        info = await self._get_contract_info(symbol)
        if info:
            return info["commission_rt"]
        # Fallback: lookup by symbol_id in defaults
        symbol_id = self._extract_symbol_id(symbol)
        return DEFAULT_COMMISSIONS.get(symbol_id, DEFAULT_COMMISSION)

    @staticmethod
    def _extract_symbol_id(contract_id: str) -> str:
        """Extract symbol_id from contract_id. E.g. 'CON.F.US.MES.H26' -> 'F.US.MES'"""
        parts = contract_id.split(".")
        if len(parts) >= 5 and parts[0] == "CON":
            return ".".join(parts[1:-1])  # F.US.MES
        return contract_id

    async def _get_contract_info(self, contract_id: str) -> Optional[dict]:
        """Get contract info from DB cache. Returns dict with all sim_contracts columns."""
        now = _time.monotonic()
        if (now - self._contract_cache_ts) > self._contract_cache_ttl:
            await self._refresh_contract_cache()
        return self._contract_cache.get(contract_id)

    async def _refresh_contract_cache(self) -> None:
        """Reload contract cache from DB."""
        if self._pool is None:
            return
        try:
            async with self._pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT * FROM sim_contracts WHERE is_active=1")
                    rows = await cur.fetchall()
            self._contract_cache = {r["contract_id"]: r for r in rows}
            self._contract_cache_ts = _time.monotonic()
        except Exception as e:
            logger.error(f"Failed to refresh contract cache: {e}")

    def _get_micro_equivalent(self, contract_id: str) -> int:
        """Get micro-equivalent multiplier for a contract (from cache or group map)."""
        cached = self._contract_cache.get(contract_id)
        if cached:
            return cached["micro_equivalent"]
        symbol_id = self._extract_symbol_id(contract_id)
        group_info = CONTRACT_GROUP_MAP.get(symbol_id)
        if group_info:
            return group_info[2]  # micro_equivalent
        return 1  # default: treat as micro

    # ── Contract sync from TopstepX API ──────────────────────────

    async def sync_contracts(self, jwt_token: str, api_base: str = "https://api.topstepx.com") -> int:
        """Sync available contracts from TopstepX API into sim_contracts table.

        Returns the number of contracts synced.
        """
        import httpx

        synced = 0
        try:
            async with httpx.AsyncClient(
                timeout=30.0,
                headers={"Authorization": f"Bearer {jwt_token}"},
            ) as client:
                # Fetch available contracts
                resp = await client.post(
                    f"{api_base}/api/Contract/available",
                    json={"live": False},
                )
                resp.raise_for_status()
                data = resp.json()
                if not data.get("success"):
                    logger.error(f"Contract/available failed: {data.get('errorMessage')}")
                    return 0

                api_contracts = data.get("contracts", [])
                logger.info(f"Contract sync: {len(api_contracts)} contracts from API")

                seen_ids: set[str] = set()
                now = datetime.now()

                for c in api_contracts:
                    contract_id = c.get("id", "")
                    if not contract_id:
                        continue
                    seen_ids.add(contract_id)

                    name = c.get("name", "")
                    description = c.get("description", "")
                    tick_size = float(c.get("tickSize", 0))
                    tick_value = float(c.get("tickValue", 0))
                    is_active = 1 if c.get("activeContract", True) else 0

                    # If API didn't provide tick data, fetch via searchById
                    if tick_size <= 0 or tick_value <= 0:
                        try:
                            detail_resp = await client.post(
                                f"{api_base}/api/Contract/searchById",
                                json={"contractId": contract_id},
                            )
                            detail_resp.raise_for_status()
                            detail = detail_resp.json()
                            if detail.get("success"):
                                contract_detail = detail.get("contract", {})
                                tick_size = float(contract_detail.get("tickSize", 0))
                                tick_value = float(contract_detail.get("tickValue", 0))
                        except Exception as e:
                            logger.debug(f"Failed to fetch detail for {contract_id}: {e}")

                    if tick_size <= 0 or tick_value <= 0:
                        logger.warning(f"Skipping {contract_id}: no valid tick data")
                        continue

                    # Derive group/micro info from symbol_id
                    symbol_id = self._extract_symbol_id(contract_id)
                    group_info = CONTRACT_GROUP_MAP.get(symbol_id)
                    if group_info:
                        contract_group, is_micro, micro_equiv = group_info
                    else:
                        # Unknown contract — classify as non-micro, group=symbol_id
                        contract_group = symbol_id.split(".")[-1] if "." in symbol_id else symbol_id
                        is_micro = False
                        micro_equiv = 10
                        logger.info(f"Unknown contract group for {contract_id} (symbol_id={symbol_id}), using group={contract_group}")

                    # Lookup commission
                    commission = DEFAULT_COMMISSIONS.get(symbol_id, DEFAULT_COMMISSION)

                    # Upsert into sim_contracts
                    async with self._pool.acquire() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute(
                                """INSERT INTO sim_contracts
                                   (contract_id, symbol_id, name, description,
                                    tick_size, tick_value, commission_rt,
                                    is_micro, micro_equivalent, contract_group,
                                    is_active, last_synced_at)
                                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                   ON DUPLICATE KEY UPDATE
                                   symbol_id=%s, name=%s, description=%s,
                                   tick_size=%s, tick_value=%s,
                                   is_micro=%s, micro_equivalent=%s, contract_group=%s,
                                   is_active=%s, last_synced_at=%s""",
                                (contract_id, symbol_id, name, description,
                                 tick_size, tick_value, commission,
                                 is_micro, micro_equiv, contract_group,
                                 is_active, now,
                                 # ON DUPLICATE KEY UPDATE values:
                                 symbol_id, name, description,
                                 tick_size, tick_value,
                                 is_micro, micro_equiv, contract_group,
                                 is_active, now),
                            )
                        await conn.commit()
                    synced += 1

                    # Also seed TopstepXAccountClient contract specs cache
                    TopstepXAccountClient.update_contract_specs(contract_id, tick_size, tick_value)

                # Mark contracts not in API response as inactive
                if seen_ids:
                    async with self._pool.acquire() as conn:
                        async with conn.cursor() as cur:
                            placeholders = ",".join(["%s"] * len(seen_ids))
                            await cur.execute(
                                f"UPDATE sim_contracts SET is_active=0 WHERE contract_id NOT IN ({placeholders}) AND is_active=1",
                                tuple(seen_ids),
                            )
                        await conn.commit()

                # Refresh cache
                await self._refresh_contract_cache()
                logger.info(f"Contract sync complete: {synced} contracts synced")

        except Exception as e:
            logger.error(f"Contract sync failed: {e}", exc_info=True)

        return synced

    async def get_active_contracts(self) -> list[dict]:
        """Get all active tradeable contracts from DB."""
        if self._pool is None:
            return []
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    """SELECT contract_id, symbol_id, name, description,
                              tick_size, tick_value, commission_rt,
                              is_micro, micro_equivalent, contract_group
                       FROM sim_contracts
                       WHERE is_active=1 AND is_tradeable=1
                       ORDER BY contract_group, is_micro DESC, name"""
                )
                return await cur.fetchall()

    # ── Order logging ────────────────────────────────────────────

    async def _log_order(self, cur, account_id: int, symbol: str, side: str,
                         quantity: int, fill_price: float, status: str = "FILLED",
                         reject_reason: str = None, fee: float = 0.0) -> None:
        await cur.execute(
            """INSERT INTO sim_orders
               (account_id, symbol, side, quantity, fill_price, fee, status, reject_reason)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (account_id, symbol, side, quantity, fill_price, fee, status, reject_reason),
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

                # Position limit in micro-equivalent units
                # (1 full-size contract = 10 micro-equivalents)
                max_micro_equiv = int(acct["starting_balance"] / 1000)
                current_exposure = sum(
                    abs(p["quantity"]) * self._get_micro_equivalent(p["symbol"])
                    for p in positions
                )
                order_exposure = quantity * self._get_micro_equivalent(symbol)
                if current_exposure + order_exposure > max_micro_equiv:
                    reason = (
                        f"Position limit: {max_micro_equiv} micro-equiv max "
                        f"(current: {current_exposure}, order adds: {order_exposure})"
                    )
                    await self._log_order(cur, acct["id"], symbol, "BUY", quantity, 0, "REJECTED", reason)
                    await conn.commit()
                    return {"success": False, "error": f"BLOCKED: {reason}"}

                # Hedging guard: reject buy if any open short (across ALL contracts)
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

                # Entry fee: half of round-trip commission × quantity
                commission_rt = await self._get_commission(symbol)
                entry_fee = (commission_rt / 2) * quantity
                new_balance = acct["balance"] - entry_fee
                if new_balance < acct["mll_floor"]:
                    reason = f"Entry fee ${entry_fee:,.2f} would breach MLL floor"
                    await self._log_order(cur, acct["id"], symbol, "BUY", quantity, 0, "REJECTED", reason)
                    await conn.commit()
                    return {"success": False, "error": f"BLOCKED: {reason}"}

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

                # Deduct entry fee from balance and day P&L
                await cur.execute(
                    """UPDATE sim_accounts SET
                       balance=%s, realized_day_pnl=realized_day_pnl-%s,
                       total_realized_pnl=total_realized_pnl-%s,
                       total_fees=total_fees+%s, daily_fees=daily_fees+%s
                       WHERE id=%s""",
                    (new_balance, entry_fee, entry_fee, entry_fee, entry_fee, acct["id"]),
                )

                await self._log_order(cur, acct["id"], symbol, "BUY", quantity, price, fee=entry_fee)
                await conn.commit()

        logger.info(f"SIM BUY: {agent_name} bought {quantity}x {symbol} @ ${price:,.2f} | Fee: ${entry_fee:,.2f}")
        return {"success": True, "fill_price": price, "quantity": quantity, "symbol": symbol, "fee": entry_fee}

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

                # Position limit in micro-equivalent units
                max_micro_equiv = int(acct["starting_balance"] / 1000)
                current_exposure = sum(
                    abs(p["quantity"]) * self._get_micro_equivalent(p["symbol"])
                    for p in positions
                )
                order_exposure = quantity * self._get_micro_equivalent(symbol)
                if current_exposure + order_exposure > max_micro_equiv:
                    reason = (
                        f"Position limit: {max_micro_equiv} micro-equiv max "
                        f"(current: {current_exposure}, order adds: {order_exposure})"
                    )
                    await self._log_order(cur, acct["id"], symbol, "SELL", quantity, 0, "REJECTED", reason)
                    await conn.commit()
                    return {"success": False, "error": f"BLOCKED: {reason}"}

                # Hedging guard: reject sell if any open long (across ALL contracts)
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

                # Entry fee: half of round-trip commission × quantity
                commission_rt = await self._get_commission(symbol)
                entry_fee = (commission_rt / 2) * quantity
                new_balance = acct["balance"] - entry_fee
                if new_balance < acct["mll_floor"]:
                    reason = f"Entry fee ${entry_fee:,.2f} would breach MLL floor"
                    await self._log_order(cur, acct["id"], symbol, "SELL", quantity, 0, "REJECTED", reason)
                    await conn.commit()
                    return {"success": False, "error": f"BLOCKED: {reason}"}

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

                # Deduct entry fee from balance and day P&L
                await cur.execute(
                    """UPDATE sim_accounts SET
                       balance=%s, realized_day_pnl=realized_day_pnl-%s,
                       total_realized_pnl=total_realized_pnl-%s,
                       total_fees=total_fees+%s, daily_fees=daily_fees+%s
                       WHERE id=%s""",
                    (new_balance, entry_fee, entry_fee, entry_fee, entry_fee, acct["id"]),
                )

                await self._log_order(cur, acct["id"], symbol, "SELL", quantity, price, fee=entry_fee)
                await conn.commit()

        logger.info(f"SIM SELL: {agent_name} sold {quantity}x {symbol} @ ${price:,.2f} | Fee: ${entry_fee:,.2f}")
        return {"success": True, "fill_price": price, "quantity": quantity, "symbol": symbol, "fee": entry_fee}

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

                # Exit fee: half of round-trip commission × closed quantity
                commission_rt = await self._get_commission(symbol)
                exit_fee = (commission_rt / 2) * close_qty

                # Update account balances
                new_balance = acct["balance"] + realized_pnl - exit_fee
                new_total_realized = acct["total_realized_pnl"] + realized_pnl - exit_fee
                new_day_pnl = acct["realized_day_pnl"] + realized_pnl - exit_fee
                is_win = realized_pnl > 0

                new_total_profit = acct["total_profit"] + (realized_pnl if realized_pnl > 0 else 0)
                new_total_loss = acct["total_loss"] + (realized_pnl if realized_pnl < 0 else 0)
                new_total_trades = acct["total_trades"] + 1
                new_daily_trades = acct["daily_trades"] + 1
                new_winning = acct["winning_trades"] + (1 if is_win else 0)
                new_losing = acct["losing_trades"] + (1 if not is_win and realized_pnl != 0 else 0)

                # High water marks (tracked for display; MLL only ratchets at EOD)
                new_highest_balance = max(acct["highest_balance"], new_balance)
                new_highest_realized = max(acct["highest_realized_balance"], new_balance)

                # Blown check — MLL floor is fixed intraday (EOD drawdown rule)
                blown = new_balance < acct["mll_floor"]
                can_trade = not blown

                await cur.execute(
                    """UPDATE sim_accounts SET
                       balance=%s, realized_day_pnl=%s, total_realized_pnl=%s,
                       total_profit=%s, total_loss=%s,
                       highest_balance=%s, highest_realized_balance=%s,
                       total_trades=%s, daily_trades=%s, winning_trades=%s, losing_trades=%s,
                       total_fees=total_fees+%s, daily_fees=daily_fees+%s,
                       can_trade=%s, blown=%s
                       WHERE id=%s""",
                    (new_balance, new_day_pnl, new_total_realized,
                     new_total_profit, new_total_loss,
                     new_highest_balance, new_highest_realized,
                     new_total_trades, new_daily_trades, new_winning, new_losing,
                     exit_fee, exit_fee,
                     can_trade, blown, acct["id"]),
                )

                # Record trade (entry_fee is half-RT charged on open; exit_fee on close)
                side = "LONG" if is_long else "SHORT"
                entry_fee_per_contract = commission_rt / 2
                opened_at = pos["created_at"]
                closed_at = datetime.now()
                duration = int((closed_at - opened_at).total_seconds()) if opened_at else 0

                await cur.execute(
                    """INSERT INTO sim_trades
                       (account_id, symbol, side, quantity, entry_price, exit_price,
                        realized_pnl, is_win, tick_size, tick_value,
                        entry_fee, exit_fee, opened_at, closed_at, duration_secs)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (acct["id"], symbol, side, close_qty, pos["avg_price"], price,
                     realized_pnl, is_win, tick_size, tick_value,
                     entry_fee_per_contract * close_qty, exit_fee,
                     opened_at, closed_at, duration),
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

                await self._log_order(cur, acct["id"], symbol, "CLOSE", close_qty, price, fee=exit_fee)
                await conn.commit()

        action = "CLOSED" if close_qty >= pos_size else f"PARTIAL CLOSE ({close_qty}/{pos_size})"
        logger.info(f"SIM {action}: {agent_name} {symbol} @ ${price:,.2f} | PnL: ${realized_pnl:+,.2f} | Fee: ${exit_fee:,.2f}")

        result = {
            "success": True,
            "fill_price": price,
            "quantity_closed": close_qty,
            "realized_pnl": realized_pnl,
            "symbol": symbol,
            "new_balance": new_balance,
            "fee": exit_fee,
        }
        if blown:
            result["blown"] = True
            result["warning"] = f"Account BLOWN — balance ${new_balance:,.2f} < MLL floor ${acct['mll_floor']:,.2f}"
            logger.warning(f"ACCOUNT BLOWN: {agent_name} | Balance: ${new_balance:,.2f} < MLL: ${acct['mll_floor']:,.2f}")
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

                # Track intraday high water mark (MLL only ratchets at EOD)
                if equity > acct["highest_unrealized_balance"]:
                    await cur.execute(
                        "UPDATE sim_accounts SET highest_unrealized_balance=%s WHERE id=%s",
                        (equity, acct["id"]),
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
                    "totalFees": acct.get("total_fees", 0),
                    "dailyFees": acct.get("daily_fees", 0),
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
                        "totalFees": acct.get("total_fees", 0),
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
                       total_fees=0, daily_fees=0,
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

    # ── End-of-session MLL ratchet ──────────────────────────────

    async def end_of_session_update(self) -> None:
        """Ratchet MLL floor and reset daily stats at end of trading session.

        EOD drawdown rule: the trailing MLL floor is only updated based on
        the end-of-day balance, not intraday highs. This runs once per day
        at session close (5 PM CT / 10 PM UTC).
        """
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT * FROM sim_accounts WHERE can_trade=1 FOR UPDATE")
                accounts = await cur.fetchall()

                for acct in accounts:
                    balance = acct["balance"]
                    new_mll_floor = max(
                        acct["mll_floor"],
                        balance - acct["drawdown_limit"],
                    )
                    await cur.execute(
                        """UPDATE sim_accounts SET
                           mll_floor=%s,
                           start_of_day_balance=%s,
                           realized_day_pnl=0,
                           daily_trades=0,
                           daily_fees=0
                           WHERE id=%s""",
                        (new_mll_floor, balance, acct["id"]),
                    )
                    if new_mll_floor != acct["mll_floor"]:
                        logger.info(
                            f"EOD MLL RATCHET: {acct['agent_name']} | "
                            f"Balance: ${balance:,.2f} | "
                            f"MLL: ${acct['mll_floor']:,.2f} -> ${new_mll_floor:,.2f}"
                        )
                await conn.commit()
        logger.info("End-of-session update complete: MLL ratcheted, daily stats reset")

    # ── Continuous MLL monitor ───────────────────────────────────

    async def check_mll_all_accounts(self) -> list[dict]:
        """Check every account with open positions for MLL breach.

        Uses EOD drawdown rules: mll_floor is fixed intraday and only
        ratchets up at end of session. Force-liquidates when equity < mll_floor.

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

                    # Track intraday high water mark (MLL only ratchets at EOD)
                    new_highest = max(acct["highest_unrealized_balance"], equity)
                    mll_floor = acct["mll_floor"]

                    if equity < mll_floor:
                        # ── MLL BREACH: force-close all positions ────
                        total_realized = 0.0
                        total_exit_fees = 0.0
                        for pos in positions:
                            live = self._get_live_price(pos["symbol"])
                            if live is None:
                                live = pos["avg_price"]  # fallback: flat close

                            is_long = pos["quantity"] > 0
                            signed_qty = pos["quantity"]
                            abs_qty = abs(pos["quantity"])
                            pnl = (
                                (live - pos["avg_price"]) / pos["tick_size"]
                            ) * pos["tick_value"] * signed_qty
                            total_realized += pnl

                            # Exit fee for force-liquidation
                            pos_commission = await self._get_commission(pos["symbol"])
                            pos_exit_fee = (pos_commission / 2) * abs_qty
                            total_exit_fees += pos_exit_fee
                            entry_fee_per_contract = pos_commission / 2

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
                                    tick_value, entry_fee, exit_fee,
                                    opened_at, closed_at, duration_secs)
                                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                                (acct["id"], pos["symbol"], side, abs_qty,
                                 pos["avg_price"], live, pnl, pnl > 0,
                                 pos["tick_size"], pos["tick_value"],
                                 entry_fee_per_contract * abs_qty, pos_exit_fee,
                                 opened_at, closed_at, duration),
                            )
                            await self._log_order(
                                cur, acct["id"], pos["symbol"], "CLOSE",
                                abs_qty, live, fee=pos_exit_fee,
                            )

                        # Delete all positions
                        await cur.execute(
                            "DELETE FROM sim_positions WHERE account_id=%s",
                            (acct["id"],),
                        )

                        # Update account (deduct exit fees from balance)
                        new_balance = acct["balance"] + total_realized - total_exit_fees
                        new_day_pnl = acct["realized_day_pnl"] + total_realized - total_exit_fees
                        new_total_realized = acct["total_realized_pnl"] + total_realized - total_exit_fees
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
                               total_trades=%s, daily_trades=%s,
                               winning_trades=%s, losing_trades=%s,
                               total_fees=total_fees+%s, daily_fees=daily_fees+%s,
                               can_trade=0, blown=1
                               WHERE id=%s""",
                            (new_balance, new_day_pnl, new_total_realized,
                             new_profit, new_loss,
                             new_balance, new_highest, new_balance,
                             new_trades, new_daily,
                             acct["winning_trades"] + wins_added,
                             acct["losing_trades"] + losses_added,
                             total_exit_fees, total_exit_fees,
                             acct["id"]),
                        )
                        await conn.commit()

                        blown_accounts.append({
                            "agent_name": acct["agent_name"],
                            "balance": new_balance,
                            "mll_floor": mll_floor,
                            "equity_at_breach": equity,
                        })
                        logger.warning(
                            f"MLL BREACH — FORCE LIQUIDATED: {acct['agent_name']} | "
                            f"Equity ${equity:,.2f} < MLL ${mll_floor:,.2f} | "
                            f"New balance: ${new_balance:,.2f}"
                        )
                    else:
                        # No breach — just update high water mark (not MLL)
                        if new_highest != acct["highest_unrealized_balance"]:
                            await cur.execute(
                                "UPDATE sim_accounts SET highest_unrealized_balance=%s WHERE id=%s",
                                (new_highest, acct["id"]),
                            )
                            await conn.commit()
                        else:
                            await conn.rollback()

        return blown_accounts

    # ── Market-close liquidation ────────────────────────────────

    async def _fetch_close_prices(self, symbols: set[str]) -> None:
        """Fetch the last close price for each symbol via TopstepX retrieveBars.

        Updates ``TopstepXAccountClient._current_prices`` so that the
        subsequent ``execute_close`` calls use a real market price instead
        of falling back to avg_price.
        """
        import os
        jwt_token = os.getenv("TOPSTEPX_JWT_TOKEN", "")
        if not jwt_token:
            logger.warning("_fetch_close_prices: no JWT token available")
            return

        api_base = os.getenv("TOPSTEPX_API_URL", "https://api.topstepx.com")
        url = f"{api_base}/api/History/retrieveBars"
        now = datetime.now(timezone.utc)

        async with httpx.AsyncClient(
            headers={"Authorization": f"Bearer {jwt_token}"},
            timeout=15.0,
        ) as client:
            for symbol in symbols:
                try:
                    payload = {
                        "contractId": symbol,
                        "live": False,
                        "startTime": (now - timedelta(minutes=30)).isoformat(),
                        "endTime": now.isoformat(),
                        "unit": 2,       # Minute
                        "unitNumber": 1,
                        "limit": 1,
                        "includePartialBar": True,
                    }
                    resp = await client.post(url, json=payload)
                    if resp.status_code == 429:
                        logger.warning(f"_fetch_close_prices: 429 for {symbol}")
                        continue
                    resp.raise_for_status()
                    data = resp.json()
                    if data.get("success"):
                        bars = data.get("bars", [])
                        if bars:
                            price = float(bars[-1]["c"])
                            TopstepXAccountClient.update_market_price(symbol, price)
                            logger.info(f"Fetched close price for {symbol}: ${price:,.2f}")
                except Exception as e:
                    logger.error(f"_fetch_close_prices failed for {symbol}: {e}")

    async def liquidate_all_positions(self) -> list[dict]:
        """Force-close ALL open positions across ALL agents at market price.

        Called at market close to ensure no positions are held overnight
        through maintenance windows. Uses avg_price as fill when no live
        price is available (P&L = 0 in that case, which is fair at close).

        Returns list of liquidation results per agent/symbol.
        """
        results: list[dict] = []
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT a.agent_name, p.symbol, p.quantity, p.avg_price "
                    "FROM sim_positions p "
                    "JOIN sim_accounts a ON p.account_id = a.id "
                    "WHERE p.quantity != 0"
                )
                open_positions = await cur.fetchall()

        # Fetch close prices for all symbols that need liquidation
        symbols_needing_price = {
            pos["symbol"] for pos in open_positions
            if self._get_live_price(pos["symbol"]) is None
        }
        if symbols_needing_price:
            await self._fetch_close_prices(symbols_needing_price)

        for pos in open_positions:
            agent = pos["agent_name"]
            symbol = pos["symbol"]
            # Last resort fallback to avg_price if API fetch also failed
            if self._get_live_price(symbol) is None:
                TopstepXAccountClient.update_market_price(symbol, float(pos["avg_price"]))
                logger.info(f"MARKET CLOSE: Using avg_price ${pos['avg_price']:,.2f} as fallback for {symbol}")
            try:
                result = await self.execute_close(agent, symbol, quantity=0)
                result["agent_name"] = agent
                results.append(result)
                if result.get("success"):
                    logger.info(
                        f"MARKET CLOSE: Liquidated {agent} {symbol} "
                        f"@ ${result.get('fill_price', 0):,.2f} "
                        f"P&L: ${result.get('realized_pnl', 0):+,.2f}"
                    )
                else:
                    logger.warning(
                        f"MARKET CLOSE: Failed to liquidate {agent} {symbol}: "
                        f"{result.get('error')}"
                    )
            except Exception as e:
                logger.error(f"MARKET CLOSE: Error liquidating {agent} {symbol}: {e}")
                results.append({"agent_name": agent, "symbol": symbol, "success": False, "error": str(e)})

        return results

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
        """End-of-session handler at ~17:00 CT (CME settlement).

        1. Records daily snapshots for equity curve history.
        2. Ratchets MLL floor based on EOD balance (EOD drawdown rule).
        3. Resets daily stats (day P&L, daily trades, daily fees).
        """
        SNAPSHOT_HOUR_UTC = 22  # ~5 PM CT (winter)

        last_snapshot_date: Optional[date] = None

        while True:
            try:
                now_utc = datetime.now(timezone.utc)
                today = now_utc.date()

                if last_snapshot_date != today and now_utc.hour >= SNAPSHOT_HOUR_UTC:
                    logger.info("End-of-session: recording snapshots + ratcheting MLL")
                    await self.daily_snapshot_all()
                    await self.end_of_session_update()
                    last_snapshot_date = today
                    logger.info(f"End-of-session complete for {today}")
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"Daily snapshot scheduler error: {e}")
            await asyncio.sleep(60)
