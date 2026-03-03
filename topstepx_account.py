"""
TopstepX Account API client.

Fetches real account data from TopstepX API including:
- Account summaries
- Positions
- Balance information
- Performance metrics
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
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
    last_updated: Optional[datetime] = None


class TopstepXAccountClient:
    """Client for fetching TopstepX account data via REST API."""
    
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
            
            # Filter out accounts that can't trade or aren't visible
            if not account_id or not is_visible or not can_trade:
                logger.debug(f"Skipping account {account_id}: canTrade={can_trade}, isVisible={is_visible}")
                return None
            
            # Fetch positions for this account
            positions = await self.get_positions(account_id)
            
            # Calculate account metrics from positions
            # Note: TopstepX API doesn't provide balance directly in search,
            # so we'll need to get it from realtime updates or account details
            balance = 0.0  # Placeholder - would come from account update events
            equity = sum(pos.market_value for pos in positions)
            unrealized_pnl = sum(pos.unrealized_pnl for pos in positions)
            realized_pnl = sum(pos.realized_pnl for pos in positions)
            
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
    
    async def get_positions(self, account_id: int | str) -> list[TopstepXPosition]:
        """Fetch positions for a specific account.
        
        Args:
            account_id: Account ID to fetch positions for
            
        Returns:
            List of positions
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
            
            positions_data = data.get("positions", [])
            positions = []
            
            for pos_data in positions_data:
                position = self._parse_position(pos_data)
                if position:
                    positions.append(position)
            
            logger.debug(f"Fetched {len(positions)} position(s) for account {account_id}")
            return positions
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching positions: {e.response.status_code}")
            logger.error(f"Response: {e.response.text}")
            return []
        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            return []
    

    
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
            #   "type": PositionTypeEnum (0=Long, 1=Short),
            #   "size": number,
            #   "averagePrice": number
            # }
            
            contract_id = data.get("contractId")
            if not contract_id:
                logger.warning("Position data missing contractId field")
                return None
            
            size = float(data.get("size", 0.0))
            avg_price = float(data.get("averagePrice", 0.0))
            position_type = data.get("type", 0)  # 0=Long, 1=Short
            
            # Calculate market value (requires current market price, which we don't have here)
            # For now, use avg_price as market price
            market_value = size * avg_price
            
            # P&L calculations would require current market price
            # Setting to 0.0 for now - should be updated with real-time data
            unrealized_pnl = 0.0
            realized_pnl = 0.0
            
            # Adjust size for short positions (make negative)
            if position_type == 1:  # Short
                size = -abs(size)
            
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
    
    async def close(self):
        """Close HTTP client."""
        await self._http_client.aclose()


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
