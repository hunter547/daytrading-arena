"""
TopstepX Account Dashboard

Displays real-time account data from TopstepX API in a Rich terminal UI.
Shows account balances, positions, and performance metrics.

Usage:
    python topstepx_dashboard.py
    # Or with explicit token:
    python topstepx_dashboard.py --token YOUR_JWT_TOKEN
"""

import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from rich.columns import Columns
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from topstepx_account import TopstepXAccount, TopstepXAccountClient

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


class TopstepXDashboard:
    """Rich Live dashboard for TopstepX accounts."""
    
    def __init__(self, client: TopstepXAccountClient, refresh_interval: float = 5.0):
        """Initialize dashboard.
        
        Args:
            client: TopstepX account client
            refresh_interval: Seconds between account refreshes
        """
        self._client = client
        self._refresh_interval = refresh_interval
        self._accounts: list[TopstepXAccount] = []
        self._last_update: Optional[datetime] = None
        self._running = False
        self.console = Console()
    
    async def start(self):
        """Start the dashboard."""
        self._running = True
        
        # Initial fetch
        await self._refresh_accounts()
        
        # Start live display
        with Live(self._build_layout(), console=self.console, refresh_per_second=1) as live:
            try:
                while self._running:
                    await asyncio.sleep(self._refresh_interval)
                    await self._refresh_accounts()
                    live.update(self._build_layout())
            except KeyboardInterrupt:
                self._running = False
                logger.info("Dashboard stopped by user")
    
    async def _refresh_accounts(self):
        """Refresh account data from API."""
        try:
            self._accounts = await self._client.get_accounts()
            self._last_update = datetime.now()
            logger.debug(f"Refreshed {len(self._accounts)} accounts")
        except Exception as e:
            logger.error(f"Error refreshing accounts: {e}")
    
    def _build_layout(self) -> Layout:
        """Build the dashboard layout."""
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="summary_header", size=1),
            Layout(name="summary", size=8),
            Layout(name="positions", ratio=1),
        )
        
        layout["header"].update(self._build_header())
        layout["summary_header"].update(
            Text.from_markup("[bold]TopstepX Account Summaries[/]", justify="center")
        )
        layout["summary"].update(self._build_summary_cards())
        layout["positions"].update(self._build_all_positions_table())
        
        return layout
    
    def _build_header(self) -> Panel:
        """Build header panel."""
        now = datetime.now().strftime("%H:%M:%S")
        last_update = self._last_update.strftime("%H:%M:%S") if self._last_update else "N/A"
        
        status = "[bold green]CONNECTED[/]" if self._accounts else "[bold yellow]FETCHING...[/]"
        
        return Panel(
            Text.from_markup(
                f"[bold cyan]TopstepX Portfolio Dashboard[/]  [bold red]●[/] {status}\n"
                f"[dim]Current Time: {now}  |  Last Update: {last_update}  |  "
                f"Accounts: {len(self._accounts)}[/]"
            ),
            style="cyan",
            height=3,
        )
    
    def _build_summary_cards(self) -> Columns:
        """Build account summary cards."""
        if not self._accounts:
            return Columns(
                [Panel("[dim]No accounts yet[/]", border_style="dim")],
                expand=True,
                equal=True,
            )
        
        cards = []
        for i, account in enumerate(self._accounts, 1):
            # Calculate total position value
            total_pos_value = sum(pos.market_value for pos in account.positions)
            total_pnl = sum(pos.unrealized_pnl for pos in account.positions)
            
            # Color for P&L
            pnl_color = "green" if total_pnl >= 0 else "red"
            pnl_sign = "+" if total_pnl >= 0 else ""
            
            card_content = (
                f"[cyan]Account ID:[/] {account.account_id}\n"
                f"[magenta]Equity:[/] ${account.equity:,.2f}\n"
                f"[yellow]Positions:[/] {len(account.positions)}  "
                f"[{pnl_color}]P&L:[/] [{pnl_color}]{pnl_sign}${total_pnl:,.2f}[/]\n"
                f"[dim]Can Trade: {'Yes' if account.name.startswith('50K') else 'Practice'}[/]"
            )
            
            card = Panel(
                Text.from_markup(card_content),
                title=f"[bold]#{i} {account.name}[/]",
                border_style="cyan",
            )
            cards.append(card)
        
        return Columns(cards, expand=True, equal=True)
    
    def _build_all_positions_table(self) -> Panel:
        """Build table showing all positions across all accounts."""
        table = Table(expand=True, show_lines=False, title="All Positions")
        table.add_column("Account", style="bold cyan", ratio=3)
        table.add_column("Symbol", style="yellow", ratio=3)
        table.add_column("Quantity", justify="right", ratio=2)
        table.add_column("Avg Price", justify="right", ratio=2)
        table.add_column("Market Value", justify="right", ratio=2)
        table.add_column("Unrealized P&L", justify="right", ratio=2)
        
        has_positions = False
        
        for account in self._accounts:
            if not account.positions:
                continue
            
            has_positions = True
            account_name_short = account.name[:20]  # Truncate long names
            
            for pos in account.positions:
                # Color for quantity (green for long, red for short)
                qty_color = "green" if pos.quantity > 0 else "red"
                
                # Color for P&L
                pnl_color = "green" if pos.unrealized_pnl >= 0 else "red"
                pnl_sign = "+" if pos.unrealized_pnl >= 0 else ""
                
                table.add_row(
                    account_name_short,
                    pos.symbol,
                    f"[{qty_color}]{pos.quantity:+.0f}[/]",
                    f"${pos.avg_price:,.2f}",
                    f"${pos.market_value:,.2f}",
                    f"[{pnl_color}]{pnl_sign}${pos.unrealized_pnl:,.2f}[/]",
                )
        
        if not has_positions:
            table.add_row(
                "[dim]No open positions[/]",
                "-",
                "-",
                "-",
                "-",
                "-",
            )
        
        return Panel(table, border_style="blue", title="[bold]Open Positions[/]")


async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="TopstepX Account Dashboard")
    parser.add_argument(
        "--token",
        type=str,
        help="JWT token (or set TOPSTEPX_JWT_TOKEN env var)",
    )
    parser.add_argument(
        "--api-url",
        type=str,
        default="https://api.topstepx.com",
        help="TopstepX API base URL",
    )
    parser.add_argument(
        "--refresh-interval",
        type=float,
        default=5.0,
        help="Refresh interval in seconds (default: 5.0)",
    )
    args = parser.parse_args()
    
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
    
    # Create client
    client = TopstepXAccountClient(jwt_token=token, api_base_url=args.api_url)
    
    try:
        # Create and start dashboard
        dashboard = TopstepXDashboard(client, refresh_interval=args.refresh_interval)
        await dashboard.start()
    finally:
        await client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nDashboard stopped")
