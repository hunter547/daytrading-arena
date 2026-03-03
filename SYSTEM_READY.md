# Futures Trading System: Ready to Launch! 🚀

## System Overview

Your multi-agent futures trading system is configured and ready to run with:

- **Market Data**: TopstepX (Live CME Futures - MES, MNQ)
- **AI Model**: GPT-5 Nano (OpenAI's latest efficient model)
- **Broker**: Kafka (Calfkit streaming framework)
- **Dashboard**: Streamlit (Real-time monitoring at http://localhost:8501)

## Quick Start (One Command)

```bash
./start_futures_trading.sh
```

**What happens:**
1. ✅ Validates Docker is running
2. ✅ Starts Kafka broker automatically
3. ✅ Connects to TopstepX SignalR (live market data)
4. ✅ Deploys tools & dashboard
5. ✅ Deploys GPT-5 Nano ChatNode
6. ✅ Deploys your first trading agent
7. 🎯 Agent starts analyzing and trading!

## Current Configuration

### Market Data
- **Provider**: TopstepX (CME Futures)
- **Contracts**: 
  - CON.F.US.MES.H26 (Micro E-mini S&P 500)
  - CON.F.US.MNQ.H26 (Micro E-mini NASDAQ)
- **Update Frequency**: Every 5 seconds
- **Data Types**: Live quotes, trades, market depth (order book)

### AI Configuration
- **Model**: GPT-5 Nano
  - Latest efficient model from OpenAI
  - Optimized for real-time trading decisions
  - Fast inference with strong reasoning
- **Starting Capital**: $100,000 virtual per agent
- **Default Strategy**: Momentum trading

### Infrastructure
- **Broker**: Kafka at localhost:9092
- **Dashboard**: Streamlit at http://localhost:8501
- **Logs**: ./logs/ directory

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     TopstepX (CME Data)                     │
│              Micro E-mini S&P 500 + NASDAQ                  │
└──────────────────────┬──────────────────────────────────────┘
                       │ SignalR WebSocket
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              Unified Market Connector                       │
│       (Converts TopstepX data → Kafka messages)             │
└──────────────────────┬──────────────────────────────────────┘
                       │ Publishes every 5s
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                   Kafka Broker                              │
│            (Event streaming backbone)                       │
└──────────┬──────────────────────────┬───────────────────────┘
           │                          │
           ▼                          ▼
┌──────────────────┐        ┌────────────────────────┐
│  Agent Router(s) │◄──────►│ ChatNode (GPT-5 Nano)  │
│  (Trading Logic) │        │   (AI Decision Making) │
└────────┬─────────┘        └────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│                   Trading Tools                             │
│  • execute_trade  • get_portfolio  • calculator             │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              Dashboard (Real-time UI)                       │
│     Positions • P&L • Trades • Performance Metrics          │
└─────────────────────────────────────────────────────────────┘
```

## What Your Agent Does

1. **Receives Market Data** (every 5 seconds)
   - Current price
   - Bid/Ask spread
   - Volume
   - Recent candlestick data
   - Order book depth

2. **Analyzes with GPT-5 Nano**
   - Evaluates market conditions
   - Applies trading strategy (momentum/mean-reversion/contrarian)
   - Calculates position sizes
   - Determines entry/exit points

3. **Executes Trades**
   - Calls `execute_trade` tool
   - Updates portfolio
   - Tracks P&L

4. **Monitors Performance**
   - All activity visible on dashboard
   - Logs reasoning to files
   - Real-time updates

## Monitoring & Control

### Dashboard
**URL**: http://localhost:8501

Shows:
- All active agents
- Current positions (long/short)
- Unrealized P&L
- Trade history
- Account balances
- Performance charts

### Logs
```bash
# Agent reasoning and decisions
tail -f logs/agent.log

# Market data flow
tail -f logs/connector.log

# Dashboard activity
tail -f logs/dashboard.log

# LLM interactions
tail -f logs/chatnode.log

# All logs at once
tail -f logs/*.log
```

### Status Check
```bash
# List running components
ps aux | grep python | grep -E "deploy_|unified_|tools_"

# Check Kafka topics
docker exec -it calfkit-broker kafka-topics --list --bootstrap-server localhost:9092
```

## Customization Examples

### Deploy Multiple Agents

```bash
# Agent 1: Momentum (already running from startup script)

# Agent 2: Mean Reversion
./run.sh python deploy_router_node.py \
  --name MeanReversion \
  --chat-node-name gpt5-nano \
  --strategy mean_reversion \
  --bootstrap-servers localhost:9092

# Agent 3: Contrarian
./run.sh python deploy_router_node.py \
  --name Contrarian \
  --chat-node-name gpt5-nano \
  --strategy contrarian \
  --bootstrap-servers localhost:9092
```

All agents will compete against each other!

### Add More Contracts

Edit `.env`:
```bash
TOPSTEPX_SYMBOLS=CON.F.US.MES.H26,CON.F.US.MNQ.H26,CON.F.US.MYM.H26
```

Then restart the connector:
```bash
pkill -f unified_market_connector
./run.sh python unified_market_connector.py --provider topstepx ...
```

### Change Update Frequency

```bash
# Update every 10 seconds instead of 5
INTERVAL=10 ./start_futures_trading.sh
```

### Use Different Model

```bash
# Use GPT-4o instead
CHAT_MODEL=gpt-4o CHAT_NODE_NAME=gpt4o ./start_futures_trading.sh
```

## Stopping the System

```bash
# Method 1: Ctrl+C in the startup terminal

# Method 2: Run stop script
./stop_futures_trading.sh

# Method 3: Manual kill
pkill -f 'python.*deploy_|python.*unified_|python.*tools_'
```

## File Structure

```
crypto-daytrading-arena/
├── start_futures_trading.sh    # 🚀 Main startup script
├── stop_futures_trading.sh     # 🛑 Shutdown script
├── QUICKSTART_FUTURES.md       # ⚡ 5-minute quick start
├── START_FUTURES_TRADING.md    # 📖 Detailed guide
├── SYSTEM_READY.md             # 📋 This file
│
├── topstepx_adapter.py         # TopstepX market data adapter
├── unified_market_connector.py # Market data → Kafka bridge
├── deploy_chat_node.py         # GPT-5 Nano deployment
├── deploy_router_node.py       # Agent deployment
├── tools_and_dashboard.py      # Trading tools + UI
│
├── logs/                       # All runtime logs
│   ├── agent.log
│   ├── connector.log
│   ├── dashboard.log
│   └── chatnode.log
│
└── .env                        # Configuration
```

## Environment Variables

All configurable via `.env` or export:

```bash
# TopstepX Configuration
TOPSTEPX_USERNAME=your_email@example.com
TOPSTEPX_API_KEY=your_api_key_here
TOPSTEPX_JWT_TOKEN=auto_generated_on_first_run
TOPSTEPX_SYMBOLS=CON.F.US.MES.H26,CON.F.US.MNQ.H26

# OpenAI Configuration
OPENAI_API_KEY=sk-...

# Kafka Configuration (optional)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Runtime Configuration (optional)
INTERVAL=5                    # Market data update frequency
CHAT_MODEL=gpt-5-nano        # AI model to use
CHAT_NODE_NAME=gpt5-nano     # ChatNode identifier
AGENT_NAME=FuturesTrader     # Agent name
STRATEGY=momentum            # Trading strategy
```

## Cost Considerations

### OpenAI API (GPT-5 Nano)
- **Pricing**: Check OpenAI pricing page for GPT-5 Nano rates
- **Typical Usage**: ~1 request per agent per market update (5s intervals)
- **Daily Estimate**: ~17,280 requests/day per agent (continuous trading)
- **Recommendation**: Set budget alerts in OpenAI dashboard

### TopstepX
- **Data**: Free with account (simulated data)
- **No real money at risk** - using sim accounts

## Trading Strategies

### Momentum
- Buys on upward trends
- Sells on downward trends
- Best in trending markets

### Mean Reversion
- Buys on dips
- Sells on rallies
- Best in ranging markets

### Contrarian
- Fades strong moves
- Takes opposite positions
- Best for counter-trend plays

*Edit strategies in `deploy_router_node.py`*

## Important Notes

⚠️ **Educational/Testing Only**
- Uses TopstepX simulated accounts
- No real money at risk
- For learning and development

⏰ **Market Hours**
- CME Futures: Sun 5pm - Fri 4pm CT
- No data outside market hours

💰 **Monitor Costs**
- Track OpenAI API usage
- Set budget limits
- Consider rate limits

🔒 **Security**
- Keep API keys secure
- Don't commit `.env` to git
- Use environment variables

## Troubleshooting

### No Data Flowing
```bash
# Test TopstepX connection
./run.sh python topstepx_tick_viewer.py

# Check market hours (must be Sun 5pm - Fri 4pm CT)
```

### Agent Not Trading
```bash
# Check agent logs
tail -f logs/agent.log

# Verify ChatNode is running
ps aux | grep deploy_chat_node

# Check market data arriving
tail -f logs/connector.log
```

### Dashboard Not Loading
```bash
# Restart dashboard
pkill -f tools_and_dashboard
./run.sh python tools_and_dashboard.py --bootstrap-servers localhost:9092

# Check logs
tail -f logs/dashboard.log
```

### Kafka Issues
```bash
# Check Kafka is running
docker ps | grep kafka

# Restart Kafka
cd ../calfkit-broker && make dev-down && make dev-up
```

## Support & Documentation

- **Quick Start**: `QUICKSTART_FUTURES.md`
- **Detailed Setup**: `START_FUTURES_TRADING.md`
- **TopstepX Guide**: `TOPSTEPX_QUICKSTART.md`
- **Adapter Docs**: `ADAPTER_README.md`
- **CLI Reference**: `CLI_REFERENCE.md`
- **Main README**: `README.md`

## Ready to Launch! 🎯

Everything is configured and ready. Just run:

```bash
./start_futures_trading.sh
```

Then open http://localhost:8501 and watch your GPT-5 Nano powered agent trade CME futures in real-time!

---

**Questions?** Check the logs in `./logs/` or review the documentation files listed above.

**Good luck!** 🚀📈
