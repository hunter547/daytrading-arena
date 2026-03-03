"""
Test what the dashboard actually renders.
"""

import asyncio
from trading_tools import view, _topstepx_client
from rich.console import Console

async def main():
    # Fetch accounts first
    if _topstepx_client:
        await view._refresh_topstepx_accounts()
        print(f"Loaded {len(view._topstepx_accounts)} TopstepX accounts")
    else:
        print("No TopstepX client")
    
    # Build and render layout
    layout = view._build_layout()
    
    # Print to console
    console = Console()
    console.print(layout)

if __name__ == "__main__":
    asyncio.run(main())
