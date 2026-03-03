# Setting Up AI Agent to Trade on TopstepX Practice Account

This guide shows you how to connect an AI agent (GPT-4, Claude, etc.) to your TopstepX practice account and start autonomous trading.

## Prerequisites ✅

Before starting, make sure you have:

- [x] TopstepX JWT token in `.env` file
- [x] Practice account (ID: 19424999)
- [x] Kafka running (`docker-compose up -d kafka`)
- [x] Python virtual environment activated

## Architecture Overview

```
┌─────────────────┐
│   AI Agent      │
│  (GPT-4/etc)    │
└────────┬────────┘
         │
         │ Uses tools via Kafka
         │
    ┌────▼─────────────────────────────┐
    │                                   │
    │  TopstepX Trading Tools Service   │
    │  - topstepx_buy()                │
    │  - topstepx_sell()               │
    │  - topstepx_portfolio()          │
    │                                   │
    └────┬──────────────────────────────┘
         │
         │ HTTPS API calls
         │
    ┌────▼─────────────────┐
    │   TopstepX API       │
    │   (Live Trading)     │
    └──────────────────────┘
         ▲
         │
    ┌────┴─────────────────┐
    │  Market Data Feed    │
    │  (Price updates)     │
    └──────────────────────┘
```

## Step-by-Step Setup

### Step 1: Start TopstepX Trading Tools Service

This service makes trading tools available to your AI agent via Kafka.

**Terminal 1:**
```bash
cd /home/hunter547/code/crypto-daytrading-arena

./run.sh python topstepx_trading_tools.py --bootstrap-servers localhost:9092
```

**Expected Output:**
```
============================================================
TopstepX Trading Tools Deployment
============================================================

Connecting to Kafka at localhost:9092...

Registering TopstepX trading tools:
  ✓ topstepx_buy - Buy futures contracts
  ✓ topstepx_sell - Sell futures contracts
  ✓ topstepx_portfolio - Get portfolio status

✓ Trading enabled on practice account: 19424999

Tools are ready for agent requests!
```

**Keep this running!** This is the bridge between your AI and TopstepX.

---

### Step 2: Start Market Data Feed (Optional but Recommended)

Provides real-time price updates to your AI agent.

**Terminal 2:**
```bash
./run.sh python unified_market_connector.py \
  --provider topstepx \
  --symbols CON.F.US.MES.H26 CON.F.US.MNQ.H26 \
  --bootstrap-servers localhost:9092 \
  --interval 5
```

**What this does:**
- Streams live prices for MES (Micro S&P) and MNQ (Micro Nasdaq)
- Updates every 5 seconds
- Publishes to Kafka for the AI to consume

**Keep this running too!**

---

### Step 3: Create Your AI Trading Agent

Now create an AI agent that can use the trading tools. Here's a complete example:

**File: `ai_trader.py`**
```python
"""
AI Trading Agent for TopstepX

This agent uses GPT-4 to analyze market data and make trading decisions
on your TopstepX practice account.
"""

import asyncio
import logging
import os
from datetime import datetime

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
KAFKA_SERVERS = "localhost:9092"

# Trading strategy prompt
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
- Close positions that are losing money
- Take profits when targets are hit
- Maximum 2 positions at once

STRATEGY:
- Analyze market data when you receive price updates
- Look for momentum and trend signals
- Use stop losses to protect capital
- Don't overtrade - wait for good setups

CONTRACT INFO:
- CON.F.US.MES.H26: Micro E-mini S&P 500 (~$5 per point, $1.25 per tick)
- CON.F.US.MNQ.H26: Micro E-mini Nasdaq-100 (~$2 per point, $0.50 per tick)

Remember: This is real trading practice. Trade carefully and learn from each trade!
"""


async def main():
    """Deploy the AI trading agent."""
    
    print("=" * 70)
    print("AI TRADING AGENT - TopstepX Practice Account")
    print("=" * 70)
    print()
    
    # Check for OpenAI API key
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ Error: OPENAI_API_KEY not found in .env")
        print("   Add your OpenAI API key to continue")
        return
    
    # Initialize Kafka broker
    print(f"Connecting to Kafka at {KAFKA_SERVERS}...")
    broker = BrokerClient(bootstrap_servers=KAFKA_SERVERS)
    
    # Create the AI chat node (GPT-4)
    print(f"Initializing AI agent: {AGENT_NAME}")
    chat_node = ChatNode(
        name=AGENT_NAME,
        system_prompt=SYSTEM_PROMPT,
        model="gpt-4o",  # or "gpt-4-turbo", "gpt-3.5-turbo", etc.
    )
    
    # Create router that connects AI to tools
    print("Setting up agent router...")
    router = AgentRouterNode(
        chat_node=chat_node,
        tool_nodes=[],  # Tools are running in separate service
        name=f"{AGENT_NAME}-router",
        system_prompt=SYSTEM_PROMPT,
    )
    
    # Start the service
    service = NodesService(broker)
    service.register_node(router)
    
    print()
    print("✓ AI Trading Agent Deployed!")
    print()
    print("Agent Details:")
    print(f"  Name: {AGENT_NAME}")
    print(f"  Model: gpt-4o")
    print(f"  Account: 19424999 (Practice)")
    print(f"  Tools: topstepx_buy, topstepx_sell, topstepx_portfolio")
    print()
    print("The agent is now listening for market data and will trade autonomously.")
    print("Press Ctrl+C to stop.")
    print()
    
    try:
        await service.run()
    except KeyboardInterrupt:
        print("\n\nAgent stopped by user.")


if __name__ == "__main__":
    asyncio.run(main())
```

---

### Step 4: Run the AI Trading Agent

**Terminal 3:**
```bash
./run.sh python ai_trader.py
```

**Expected Output:**
```
======================================================================
AI TRADING AGENT - TopstepX Practice Account
======================================================================

Connecting to Kafka at localhost:9092...
Initializing AI agent: futures-trader
Setting up agent router...

✓ AI Trading Agent Deployed!

Agent Details:
  Name: futures-trader
  Model: gpt-4o
  Account: 19424999 (Practice)
  Tools: topstepx_buy, topstepx_sell, topstepx_portfolio

The agent is now listening for market data and will trade autonomously.
Press Ctrl+C to stop.
```

---

### Step 5: Monitor Your AI Trading (Optional)

Watch your AI trade in real-time!

**Terminal 4:**
```bash
./run.sh python tools_and_dashboard.py --bootstrap-servers localhost:9092
```

This shows:
- Your TopstepX practice account balance
- Current positions
- Trade log
- P&L chart

---

## What Happens Next?

1. **Market Data Arrives**: Price updates stream to your AI every 5 seconds
2. **AI Analyzes**: GPT-4 analyzes the market data
3. **AI Decides**: Based on its strategy, the AI decides to:
   - Check portfolio: `topstepx_portfolio()`
   - Buy: `topstepx_buy("CON.F.US.MES.H26", 1)`
   - Sell: `topstepx_sell("CON.F.US.MES.H26", 1)`
4. **Trade Executes**: Order is placed on TopstepX
5. **Dashboard Updates**: You see the trade in real-time

---

## Testing Without Auto-Trading

Want to test the agent without it making trades automatically? Create a **manual mode agent**:

```python
SYSTEM_PROMPT = """You are an AI futures trading ADVISOR (not trader).

Analyze market data and SUGGEST trades, but DO NOT execute them.

Instead, explain:
- What you would trade and why
- Entry and exit points
- Risk/reward analysis

Only use topstepx_portfolio() to check positions.
DO NOT use topstepx_buy() or topstepx_sell() unless explicitly asked.
"""
```

---

## Configuration Options

### Change the AI Model

In `ai_trader.py`, modify:
```python
chat_node = ChatNode(
    name=AGENT_NAME,
    system_prompt=SYSTEM_PROMPT,
    model="gpt-4o",  # ← Change this
)
```

**Available Models:**
- `"gpt-4o"` - GPT-4 Omni (recommended)
- `"gpt-4-turbo"` - GPT-4 Turbo
- `"gpt-4"` - GPT-4
- `"gpt-3.5-turbo"` - Cheaper, faster, less capable
- `"claude-3-opus"` - Anthropic Claude (requires different setup)

### Adjust Trading Strategy

Modify the `SYSTEM_PROMPT` to change:
- Risk tolerance
- Position sizing
- Trading frequency
- Technical indicators to use
- Entry/exit rules

### Monitor Different Contracts

In the market data connector:
```bash
./run.sh python unified_market_connector.py \
  --provider topstepx \
  --symbols CON.F.US.ES.H26 CON.F.US.NQ.H26 \  # Full-size E-minis
  --bootstrap-servers localhost:9092
```

---

## Safety Features

✅ **Practice Account Only**: Tools automatically use practice account (19424999)
✅ **Market Orders**: Fast execution, no lingering orders
✅ **Position Limits**: Configurable in system prompt
✅ **Real-time Monitoring**: Dashboard shows all activity
✅ **Easy Kill Switch**: Ctrl+C stops the agent immediately

---

## Troubleshooting

### "No market data received"
- Make sure Terminal 2 (market data connector) is running
- Check that Kafka is running: `docker ps`

### "Tool calls not working"
- Verify Terminal 1 (trading tools service) is running
- Check logs for errors

### "OpenAI API error"
- Verify `OPENAI_API_KEY` is set in `.env`
- Check your OpenAI account has credits

### "Account not found"
- Make sure `TOPSTEPX_JWT_TOKEN` is set
- Token may have expired (24hr lifetime) - run `./run.sh python topstepx_auth.py`

---

## Example: Full System Running

**You should have 4 terminals:**

1. **Trading Tools** - `topstepx_trading_tools.py` ✓
2. **Market Data** - `unified_market_connector.py` ✓
3. **AI Agent** - `ai_trader.py` ✓
4. **Dashboard** - `tools_and_dashboard.py` ✓

---

## Advanced: Custom Strategies

### Create a Scalping Agent
```python
SYSTEM_PROMPT = """You are a futures SCALPING agent.

STRATEGY:
- Take small profits (5-10 ticks)
- Quick entries and exits
- Trade 3-5 times per session
- Use tight stop losses (3-5 ticks)
"""
```

### Create a Trend Following Agent
```python
SYSTEM_PROMPT = """You are a TREND FOLLOWING agent.

STRATEGY:
- Identify strong trends
- Enter on pullbacks
- Hold for larger moves (20-50 points)
- Use trailing stops
"""
```

### Create a Mean Reversion Agent
```python
SYSTEM_PROMPT = """You are a MEAN REVERSION agent.

STRATEGY:
- Identify overbought/oversold conditions
- Trade reversals back to mean
- Use support/resistance levels
- Quick exits when momentum shifts
"""
```

---

## Next Steps

1. ✅ **Start with observation** - Let it run in advisor mode first
2. ✅ **Enable small trades** - Start with 1 contract
3. ✅ **Monitor closely** - Watch the dashboard
4. ✅ **Analyze results** - Review trades and improve prompts
5. ✅ **Scale up gradually** - Increase position sizes as you gain confidence

---

## Resources

- **Trading Tools Docs**: `TOPSTEPX_AGENT_TRADING.md`
- **API Reference**: `TOPSTEPX_CURL_EXAMPLES.md`
- **Account Filtering**: `ACCOUNT_FILTERING.md`
- **Test Tools**: `./run.sh python test_topstepx_tools.py`

**Need Help?**
- Check logs in each terminal
- Run dashboard to see current state
- Use `./run.sh python topstepx_account.py` to verify account status

---

## ⚠️ Important Reminders

- This is a **practice account** - perfect for learning
- AI can make mistakes - monitor carefully
- Start with small positions
- You can always press Ctrl+C to stop
- Review trades regularly to improve your prompts

**Happy Trading! 🚀**
