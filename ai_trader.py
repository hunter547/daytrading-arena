"""
AI Trading Agent for TopstepX Practice Account

Autonomous futures trading agent using GPT-4 to analyze market data
and execute trades on TopstepX practice account.

Usage:
    ./run.sh python ai_trader.py --bootstrap-servers localhost:9092
"""

import argparse
import asyncio
import logging
import os
import sys

from dotenv import load_dotenv

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.chat_node import ChatNode
from calfkit.runners.service import NodesService

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ============================================================================
# AGENT CONFIGURATION
# ============================================================================

AGENT_NAME = "futures-trader"

# AI Trading Strategy
SYSTEM_PROMPT = """You are an AI futures trading agent with access to a TopstepX practice account.

ACCOUNT DETAILS:
- Account ID: 19424999 (Practice Account)
- Starting Balance: $150,000
- Available Contracts: MES (Micro E-mini S&P 500), MNQ (Micro E-mini Nasdaq-100)

YOUR TOOLS:
1. topstepx_buy(contract, quantity) - Go LONG or close SHORT positions
2. topstepx_sell(contract, quantity) - Go SHORT or close LONG positions  
3. topstepx_portfolio() - Check your current positions and P&L

TRADING RULES:
- Start with 1 contract positions only
- Use proper risk management
- Always check your portfolio before trading
- Close positions that are losing more than $100
- Take profits when you're up more than $150
- Maximum 2 positions at once
- Don't trade within first 5 minutes of receiving market data (let trends develop)

STRATEGY:
- When you receive market price updates, analyze the trend
- Look for strong momentum in one direction
- Enter trades when you see clear signals
- Use the bid/ask spread to time entries
- Monitor your positions and manage risk
- If a position moves against you, consider exiting

CONTRACT INFO:
- CON.F.US.MES.H26: Micro E-mini S&P 500 (~$5 per point, $1.25 per tick)
- CON.F.US.MNQ.H26: Micro E-mini Nasdaq-100 (~$2 per point, $0.50 per tick)

IMPORTANT:
- This is real practice trading - be thoughtful
- Check your portfolio frequently
- Don't overtrade - quality over quantity
- Learn from each trade

When you receive market data:
1. First, check your current portfolio
2. Analyze the price action
3. Make a decision: enter, exit, or hold
4. Explain your reasoning briefly
"""


async def main():
    """Deploy the AI trading agent."""
    
    parser = argparse.ArgumentParser(description="AI Trading Agent for TopstepX")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="gpt-4o",
        help="OpenAI model to use (gpt-4o, gpt-4-turbo, gpt-3.5-turbo)",
    )
    parser.add_argument(
        "--advisor-mode",
        action="store_true",
        help="Run in advisor mode (analyze but don't trade)",
    )
    args = parser.parse_args()
    
    print("=" * 70)
    print("AI TRADING AGENT - TopstepX Practice Account")
    print("=" * 70)
    print()
    
    # Check for required environment variables
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ Error: OPENAI_API_KEY not found in .env")
        print("   Add your OpenAI API key to continue")
        print()
        print("   Example .env entry:")
        print("   OPENAI_API_KEY=sk-...")
        sys.exit(1)
    
    if not os.getenv("TOPSTEPX_JWT_TOKEN"):
        print("❌ Error: TOPSTEPX_JWT_TOKEN not found in .env")
        print("   Make sure trading tools service has access to TopstepX")
        sys.exit(1)
    
    # Modify system prompt for advisor mode
    system_prompt = SYSTEM_PROMPT
    if args.advisor_mode:
        system_prompt = """You are an AI futures trading ADVISOR (not active trader).

Analyze market data and SUGGEST trades, but DO NOT execute them.

When you receive market updates:
1. Check the portfolio status with topstepx_portfolio()
2. Analyze the price action and trends
3. Explain what you would do and why
4. Provide entry/exit recommendations
5. Discuss risk/reward

DO NOT use topstepx_buy() or topstepx_sell() in advisor mode.
Only observe and advise.
""" + SYSTEM_PROMPT.split("CONTRACT INFO:")[1]
    
    # Initialize Kafka broker
    print(f"Connecting to Kafka at {args.bootstrap_servers}...")
    broker = BrokerClient(bootstrap_servers=args.bootstrap_servers)
    
    # Create the AI chat node
    mode_str = "ADVISOR" if args.advisor_mode else "AUTONOMOUS TRADER"
    print(f"Initializing AI agent: {AGENT_NAME} ({mode_str})")
    print(f"Model: {args.model}")
    
    chat_node = ChatNode(
        name=AGENT_NAME,
        system_prompt=system_prompt,
        model=args.model,
    )
    
    # Create router that connects AI to tools
    print("Setting up agent router...")
    router = AgentRouterNode(
        chat_node=chat_node,
        tool_nodes=[],  # Tools are running in separate service
        name=f"{AGENT_NAME}-router",
        system_prompt=system_prompt,
    )
    
    # Start the service
    service = NodesService(broker)
    service.register_node(router)
    
    print()
    print("✓ AI Trading Agent Deployed!")
    print()
    print("Agent Configuration:")
    print(f"  Name: {AGENT_NAME}")
    print(f"  Model: {args.model}")
    print(f"  Mode: {mode_str}")
    print(f"  Account: 19424999 (Practice)")
    print(f"  Available Tools: topstepx_buy, topstepx_sell, topstepx_portfolio")
    print()
    
    if args.advisor_mode:
        print("🔍 ADVISOR MODE: Agent will analyze but not execute trades")
    else:
        print("⚡ AUTONOMOUS MODE: Agent will execute trades automatically")
        print("   (Make sure trading tools service is running!)")
    
    print()
    print("The agent is now listening for market data.")
    print("Press Ctrl+C to stop.")
    print()
    print("Tip: Run the dashboard in another terminal to monitor:")
    print("  ./run.sh python tools_and_dashboard.py --bootstrap-servers localhost:9092")
    print()
    
    try:
        await service.run()
    except KeyboardInterrupt:
        print("\n\n" + "=" * 70)
        print("Agent stopped by user.")
        print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
