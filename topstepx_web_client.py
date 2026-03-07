"""
TopstepX Web Client — wraps the userapi.topstepx.com endpoints.
TopstepDashboardClient — wraps the api.topstep.com dashboard endpoints.

These are the same APIs that the TopstepX web/desktop app and the Topstep
dashboard use internally. They provide richer data than the public
api.topstepx.com endpoints (trailing MLL, win rate, balance history, etc.).

Usage:
    client = TopstepXWebClient(jwt_token)
    accounts = await client.get_trading_accounts()
    active = [a for a in accounts if a.status == 0]

    dash = TopstepDashboardClient(email, password)
    await dash.login()
    stats = await dash.get_account_stats(account_id, "2026-03-01:2026-03-07")
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://userapi.topstepx.com"

# Headers the web app sends — version can be bumped as needed
_DEFAULT_HEADERS = {
    "Accept": "application/json",
}


@dataclass
class WebTradingAccount:
    """Account data from the userapi /TradingAccount endpoint."""

    # Identity
    account_id: int
    account_name: str
    nickname: Optional[str]
    user_id: int
    template_id: int
    type: int
    status: int  # 0=active, 6=blown/inactive
    ineligible: bool

    # Balances
    starting_balance: float
    balance: float
    start_of_day_balance: float
    max_margin: float

    # PnL
    profit_and_loss: float
    total_profit: float
    total_loss: float
    realized_day_pnl: float
    open_pnl: float
    daily_loss: float

    # High water marks
    highest_balance: float
    highest_unrealized_balance: float
    highest_realized_balance: float

    # Loss limits
    maximum_loss: float  # trailing MLL floor (computed by server)
    drawdown_limit: float

    # Trade stats
    total_trades: int
    daily_trades: int
    win_rate: float

    @classmethod
    def from_api(cls, d: dict) -> "WebTradingAccount":
        return cls(
            account_id=d["accountId"],
            account_name=d["accountName"],
            nickname=d.get("nickname"),
            user_id=d["userId"],
            template_id=d.get("templateId", 0),
            type=d.get("type", 0),
            status=d.get("status", 0),
            ineligible=d.get("ineligible", False),
            starting_balance=d.get("startingBalance", 0.0),
            balance=d.get("balance", 0.0),
            start_of_day_balance=d.get("startOfDayBalance", 0.0),
            max_margin=d.get("maxMargin", 0.0),
            profit_and_loss=d.get("profitAndLoss", 0.0),
            total_profit=d.get("totalProfit", 0.0),
            total_loss=d.get("totalLoss", 0.0),
            realized_day_pnl=d.get("realizedDayPnl", 0.0),
            open_pnl=d.get("openPnl", 0.0),
            daily_loss=d.get("dailyLoss", 0.0),
            highest_balance=d.get("highestBalance", 0.0),
            highest_unrealized_balance=d.get("highestUnrealizedBalance", 0.0),
            highest_realized_balance=d.get("highestRealizedBalance", 0.0),
            maximum_loss=d.get("maximumLoss", 0.0),
            drawdown_limit=d.get("drawDownLimit", 0.0),
            total_trades=d.get("totalTrades", 0),
            daily_trades=d.get("dailyTrades", 0),
            win_rate=d.get("winRate", 0.0),
        )


class TopstepXWebClient:
    """Client for the TopstepX web/desktop API (userapi.topstepx.com).

    Uses the same JWT token as the public API. Add new endpoint methods
    here as we discover more useful web-client calls.
    """

    def __init__(self, jwt_token: str, base_url: str = BASE_URL):
        self._base_url = base_url.rstrip("/")
        self._http = httpx.AsyncClient(
            timeout=15.0,
            headers={
                **_DEFAULT_HEADERS,
                "Authorization": f"Bearer {jwt_token}",
            },
        )

    async def _get(self, path: str) -> list | dict:
        """GET helper with error handling."""
        url = f"{self._base_url}{path}"
        resp = await self._http.get(url)
        resp.raise_for_status()
        return resp.json()

    async def _post(self, path: str, payload: dict | None = None) -> list | dict:
        """POST helper with error handling."""
        url = f"{self._base_url}{path}"
        resp = await self._http.post(url, json=payload or {})
        resp.raise_for_status()
        return resp.json()

    # ── Endpoints ──────────────────────────────────────────────────

    async def get_trading_accounts(self) -> list[WebTradingAccount]:
        """Fetch all trading accounts (active + inactive).

        Returns richer data than the public /api/Account/search endpoint,
        including trailing MLL floor, win rate, and high-water marks.
        """
        data = await self._get("/TradingAccount")
        if not isinstance(data, list):
            logger.error(f"Unexpected /TradingAccount response: {type(data)}")
            return []
        return [WebTradingAccount.from_api(item) for item in data]

    async def get_active_practice_account(self) -> Optional[WebTradingAccount]:
        """Convenience: return the first active PRAC account, if any."""
        accounts = await self.get_trading_accounts()
        for acct in accounts:
            if acct.status == 0 and "PRAC" in acct.account_name:
                return acct
        return None

    async def close(self):
        await self._http.aclose()


# ── Topstep Dashboard API (api.topstep.com) ────────────────────────


DASHBOARD_BASE_URL = "https://api.topstep.com"


@dataclass
class BalanceHistoryEntry:
    """One day's balance snapshot."""
    trade_day: str  # ISO date
    balance: float
    daily_profit: float

    @classmethod
    def from_api(cls, d: dict) -> "BalanceHistoryEntry":
        return cls(
            trade_day=d["tradeDay"],
            balance=d.get("balance", 0.0),
            daily_profit=d.get("dailyProfit", 0.0),
        )


@dataclass
class MllHistoryEntry:
    """One day's MLL floor snapshot."""
    trade_day: str
    max_loss_limit: float

    @classmethod
    def from_api(cls, d: dict) -> "MllHistoryEntry":
        return cls(
            trade_day=d["tradeDay"],
            max_loss_limit=d.get("maxLossLimit", 0.0),
        )


@dataclass
class PerformanceMetrics:
    win_rate: float
    profit_factor: float
    average_win: float
    average_loss: float
    max_drawdown: float
    sharpe_ratio: float
    total_profit: float
    total_loss: float

    @classmethod
    def from_api(cls, d: dict) -> "PerformanceMetrics":
        return cls(
            win_rate=d.get("winRate", 0.0),
            profit_factor=d.get("profitFactor", 0.0),
            average_win=d.get("averageWin", 0.0),
            average_loss=d.get("averageLoss", 0.0),
            max_drawdown=d.get("maxDrawdown", 0.0),
            sharpe_ratio=d.get("sharpeRatio", 0.0),
            total_profit=d.get("totalProfit", 0.0),
            total_loss=d.get("totalLoss", 0.0),
        )


@dataclass
class TradeDuration:
    duration: str  # e.g. "0-5m", "5-10m"
    count: int
    success_rate: float

    @classmethod
    def from_api(cls, d: dict) -> "TradeDuration":
        return cls(
            duration=d["duration"],
            count=d.get("count", 0),
            success_rate=d.get("successRate", 0.0),
        )


@dataclass
class AccountStats:
    """Full account stats from /me/accounts/{id}/stats."""
    starting_balance: float
    max_drawdown: float
    current_max_loss_limit: float
    today_pnl: float
    target_balance: Optional[float] = None
    balance_history: list[BalanceHistoryEntry] = field(default_factory=list)
    mll_history: list[MllHistoryEntry] = field(default_factory=list)
    performance: Optional[PerformanceMetrics] = None
    trade_durations: list[TradeDuration] = field(default_factory=list)

    @classmethod
    def from_api(cls, d: dict) -> "AccountStats":
        # Extract target balance from funding metrics
        fm = d.get("fundingMetrics") or {}
        raw_target = fm.get("targetBalance")
        target_bal = float(raw_target) if raw_target else None

        return cls(
            starting_balance=d.get("startingBalance", 0.0),
            max_drawdown=d.get("maxDrawdown", 0.0),
            current_max_loss_limit=d.get("currentMaxLossLimit", 0.0),
            today_pnl=d.get("todayPnl", 0.0),
            target_balance=target_bal,
            balance_history=[
                BalanceHistoryEntry.from_api(e)
                for e in d.get("balanceHistory", [])
            ],
            mll_history=[
                MllHistoryEntry.from_api(e)
                for e in d.get("maxLossLimitHistory", [])
            ],
            performance=(
                PerformanceMetrics.from_api(d["performanceMetrics"])
                if d.get("performanceMetrics")
                else None
            ),
            trade_durations=[
                TradeDuration.from_api(e)
                for e in d.get("tradeDurations", [])
            ],
        )


class TopstepDashboardClient:
    """Client for the Topstep dashboard API (api.topstep.com).

    This is a SEPARATE auth system from TopstepX. Uses Cognito login with
    email-based MFA, then short-lived access tokens (15 min) refreshed via
    a cookie-based refresh token (7 days, rotated on each use).

    Auth flow:
        1. POST /auth/cognito/login       {email, password}  -> {session}
        2. POST /auth/cognito/verify-mfa   {email, mfaCode, session, twoFactorType}
                                           -> {token} + Set-Cookie: refresh_token
        3. POST /auth/refresh-token        Cookie: refresh_token=...
                                           -> {token} + Set-Cookie: refresh_token

    For automated use, provide a refresh token via TOPSTEP_REFRESH_TOKEN env var
    (obtained from a browser login). The client will auto-refresh and rotate it.
    """

    # Required headers to pass the WAF
    _BROWSER_HEADERS = {
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64; rv:137.0) "
            "Gecko/20100101 Firefox/137.0"
        ),
        "Origin": "https://dashboard.topstep.com",
        "Referer": "https://dashboard.topstep.com/",
    }

    # Where to persist the rotated refresh token so it survives restarts
    _TOKEN_FILE = Path("/app/logs/.topstep_refresh_token")

    def __init__(
        self,
        refresh_token: Optional[str] = None,
        base_url: str = DASHBOARD_BASE_URL,
        token_file: Optional[Path] = None,
    ):
        self._base_url = base_url.rstrip("/")
        self._access_token: Optional[str] = None
        self._token_file = token_file or self._TOKEN_FILE

        # Load token: explicit arg > persisted file > None
        if refresh_token:
            self._refresh_token = refresh_token
        else:
            self._refresh_token = self._load_persisted_token()

        self._http = httpx.AsyncClient(
            timeout=15.0,
            headers=self._BROWSER_HEADERS,
        )

    # ── Token persistence ─────────────────────────────────────────

    def _load_persisted_token(self) -> Optional[str]:
        """Load the refresh token from disk."""
        try:
            if self._token_file.exists():
                token = self._token_file.read_text().strip()
                if token:
                    logger.info(f"TopstepDashboardClient: loaded refresh token from {self._token_file}")
                    return token
        except Exception as e:
            logger.warning(f"TopstepDashboardClient: failed to load token file: {e}")
        return None

    def _persist_token(self, token: str) -> None:
        """Write the latest refresh token to disk."""
        try:
            self._token_file.parent.mkdir(parents=True, exist_ok=True)
            self._token_file.write_text(token)
            logger.debug(f"TopstepDashboardClient: persisted refresh token to {self._token_file}")
        except Exception as e:
            logger.warning(f"TopstepDashboardClient: failed to persist token: {e}")

    # ── Auth ───────────────────────────────────────────────────────

    async def refresh_access_token(self) -> bool:
        """Use the refresh token cookie to get a new access token.

        Also rotates the refresh token (server sets a new cookie).
        Returns True on success.
        """
        if not self._refresh_token:
            logger.error("TopstepDashboardClient: no refresh token available")
            return False

        url = f"{self._base_url}/auth/refresh-token"
        resp = await self._http.post(
            url,
            cookies={"refresh_token": self._refresh_token},
        )
        resp.raise_for_status()
        data = resp.json()

        new_access = data.get("token")
        if not new_access:
            logger.error("TopstepDashboardClient: refresh returned no token")
            return False

        self._access_token = new_access
        self._http.headers["Authorization"] = f"Bearer {new_access}"

        # Rotate refresh token from Set-Cookie and persist
        for key, value in resp.cookies.items():
            if key == "refresh_token" and value:
                self._refresh_token = value
                self._persist_token(value)
                logger.debug("TopstepDashboardClient: refresh token rotated and persisted")
                break

        logger.info("TopstepDashboardClient: access token refreshed")
        return True

    async def _ensure_auth(self) -> None:
        """Ensure we have a valid access token, refreshing if needed."""
        if self._access_token:
            return
        await self.refresh_access_token()

    async def _get(self, path: str) -> dict:
        """GET with auto-refresh on 401."""
        await self._ensure_auth()
        url = f"{self._base_url}{path}"
        resp = await self._http.get(url)
        if resp.status_code == 401:
            self._access_token = None
            ok = await self.refresh_access_token()
            if not ok:
                resp.raise_for_status()
            resp = await self._http.get(url)
        resp.raise_for_status()
        return resp.json()

    # ── Endpoints ──────────────────────────────────────────────────

    async def get_accounts(self) -> list[dict]:
        """Fetch all accounts with both dashboard and TopstepX IDs.

        Each account dict includes:
            id: Topstep dashboard account ID
            projectXAccountId: TopstepX account ID
            platformAccount: Account name (e.g. "PRAC-V2-157469-20785082")
            active, status, stage, balance, startingBalance, etc.
        """
        data = await self._get(
            "/me/accounts/basic?offset=0&limit=50&sortBy=createdAt&sortOrder=desc"
        )
        return data.get("accounts", [])

    async def find_dashboard_account_id(self, topstepx_account_id: int) -> Optional[int]:
        """Map a TopstepX account ID to the corresponding dashboard account ID."""
        accounts = await self.get_accounts()
        for acct in accounts:
            if acct.get("projectXAccountId") == topstepx_account_id:
                return acct["id"]
        return None

    async def get_account_stats(
        self,
        account_id: int,
        time_range: str,
    ) -> AccountStats:
        """Fetch account stats with balance/MLL history.

        Args:
            account_id: Topstep dashboard account ID
            time_range: Date range string, e.g. "2026-03-01:2026-03-07"
        """
        data = await self._get(
            f"/me/accounts/{account_id}/stats?timeRange={time_range}"
        )
        return AccountStats.from_api(data)

    @property
    def current_refresh_token(self) -> Optional[str]:
        """The latest refresh token (may have been rotated)."""
        return self._refresh_token

    async def close(self):
        await self._http.aclose()
