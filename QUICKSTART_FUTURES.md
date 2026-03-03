# Quick Start: Futures Trading System 🚀

Get the multi-agent futures trading system running in under 5 minutes!

## Prerequisites

1. **Docker Desktop** is running
2. **Environment configured** (`.env` file with TopstepX and OpenAI credentials)
3. **Dependencies installed** (`uv sync` already done)

## One-Command Start

```bash
./start_futures_trading.sh
```

That's it! This script will:
- ✅ Check Docker is running
- ✅ Start Kafka broker (if needed)
- ✅ Start TopstepX market data connector
- ✅ Deploy tools and dashboard
- ✅ Deploy ChatNode with GPT-5 Nano
- ✅ Deploy your first trading agent

## Monitor the System

### Dashboard (Recommended)
Open in your browser: **http://localhost:8501**

Shows:
- Agent positions and P&L
- Trade history
- Account balances
- Real-time updates

### View Logs

```bash
# See what the agent is thinking
tail -f logs/agent.log

# See market data flowing in
tail -f logs/connector.log

# See all logs at once
tail -f logs/*.log
```

### Check Status

```bash
# List all running components
ps aux | grep python | grep -E "deploy_|unified_|tools_"

# Check PIDs
cat logs/pids.txt
```

## Stop the System

```bash
# Method 1: Press Ctrl+C in the start script terminal

# Method 2: Run stop script
./stop_futures_trading.sh

# Method 3: Kill manually
pkill -f 'python.*deploy_|python.*unified_|python.*tools_'
```

## Customization

### Change Trading Symbols

Edit `.env`:
```bash
TOPSTEPX_SYMBOLS=CON.F.US.MES.H26,CON.F.US.MNQ.H26,CON.F.US.MYM.H26
```

### Use Different LLM Model

```bash
# Use GPT-4o instead of GPT-5 Nano
CHAT_MODEL=gpt-4o CHAT_NODE_NAME=gpt4o ./start_futures_trading.sh

# Use GPT-4o-mini (cheaper than GPT-5 Nano)
CHAT_MODEL=gpt-4o-mini CHAT_NODE_NAME=gpt4o-mini ./start_futures_trading.sh
```

### Change Agent Strategy

```bash
# Mean reversion strategy
STRATEGY=mean_reversion ./start_futures_trading.sh

# Contrarian strategy
STRATEGY=contrarian ./start_futures_trading.sh
```

### Deploy Multiple Agents

After starting the system, open new terminals and run:

```bash
# Agent 2
./run.sh python deploy_router_node.py \
  --name Agent2 \
  --chat-node-name gpt5-nano \
  --strategy mean_reversion \
  --bootstrap-servers localhost:9092

# Agent 3
./run.sh python deploy_router_node.py \
  --name Agent3 \
  --chat-node-name gpt5-nano \
  --strategy contrarian \
  --bootstrap-servers localhost:9092
```

All agents will appear on the dashboard automatically!

## Manual Step-by-Step (If You Prefer)

If you want more control, see the detailed guide: [START_FUTURES_TRADING.md](START_FUTURES_TRADING.md)

## Troubleshooting

### Docker not running
```bash
# Start Docker Desktop from Applications (macOS)
# or: sudo systemctl start docker (Linux)
```

### No market data
```bash
# Test TopstepX connection directly
./run.sh python topstepx_tick_viewer.py

# Check market hours (CME: Sun 5pm - Fri 4pm CT)
```

### Kafka not starting
```bash
# Check Docker containers
docker ps

# Restart Kafka manually
cd ../calfkit-broker && make dev-down && make dev-up
```

### Dashboard not loading
```bash
# Check if running
ps aux | grep tools_and_dashboard

# Restart manually
./run.sh python tools_and_dashboard.py --bootstrap-servers localhost:9092
```

### Agent not trading
```bash
# Check agent logs
tail -f logs/agent.log

# Enable response viewer to see reasoning
START_VIEWER=true ./start_futures_trading.sh
```

## Environment Variables

All configurable via environment variables:

```bash
# Trading symbols
TOPSTEPX_SYMBOLS="CON.F.US.MES.H26,CON.F.US.MNQ.H26"

# Market data update frequency (seconds)
INTERVAL=5

# LLM model
CHAT_MODEL="gpt-5-nano"
CHAT_NODE_NAME="gpt5-nano"

# Agent configuration
AGENT_NAME="FuturesTrader"
STRATEGY="momentum"

# Start response viewer
START_VIEWER=true
```

Example:
```bash
INTERVAL=10 STRATEGY=contrarian ./start_futures_trading.sh
```

## What's Happening Under the Hood

```
TopstepX SignalR
       ↓ (live tick data)
Unified Connector
       ↓ (publishes to Kafka every 5s)
Kafka Broker
       ↓ (fan-out to agents)
Agent Router(s)
       ↓ (send to LLM with tools)
ChatNode (GPT-5 Nano)
       ↓ (returns actions)
Trading Tools
       ↓ (execute trades)
Dashboard (updates in real-time)
```

## Next Steps

1. **Watch the dashboard** - See your agent make its first trades
2. **Check the logs** - Understand what the agent is thinking
3. **Deploy more agents** - Create competition between strategies
4. **Tweak strategies** - Edit `deploy_router_node.py` to customize behavior
5. **Monitor performance** - Track which strategies perform best

## Important Notes

- 📊 **Simulated trading** - Uses TopstepX sim accounts (no real money)
- ⏰ **Market hours** - Data only flows when CME markets are open
- 💰 **Costs** - Monitor your OpenAI API usage
- 🔒 **Safety** - Agents have $100k virtual starting balance per `trading_tools.py`

## Support

- Full guide: [START_FUTURES_TRADING.md](START_FUTURES_TRADING.md)
- TopstepX setup: [TOPSTEPX_QUICKSTART.md](TOPSTEPX_QUICKSTART.md)
- Architecture details: [README.md](README.md)
- Adapter docs: [ADAPTER_README.md](ADAPTER_README.md)

Happy Trading! 🎯
