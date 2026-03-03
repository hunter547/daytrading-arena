# Starting the Futures Trading System with TopstepX

This guide will walk you through starting the complete multi-agent futures trading system using live market data from TopstepX (CME futures).

## Architecture Overview

```
TopstepX Market Data (SignalR)
        ↓
Unified Market Connector
        ↓
Kafka Broker ←→ Agent Router(s) ←→ ChatNode(s) (GPT-4o-mini)
        ↓
Tools & Dashboard
```

## Prerequisites Checklist

- [x] Docker Desktop installed and running
- [x] Python 3.10+ with virtual environment
- [x] TopstepX account with API credentials
- [x] OpenAI API key (for GPT-4o-mini or GPT-4)
- [x] All dependencies installed (`uv sync` or `pip install -r requirements.txt`)

## Step-by-Step Startup

### Step 1: Start Docker Desktop

Make sure Docker Desktop is running:

```bash
# Check if Docker is running
docker ps

# If not, start Docker Desktop manually from your Applications
```

### Step 2: Start the Kafka Broker

Clone and start the Kafka broker:

```bash
# Navigate to parent directory
cd ~/code

# Clone the calfkit-broker repo
git clone https://github.com/calf-ai/calfkit-broker

# Start the broker
cd calfkit-broker
make dev-up
```

Wait for the broker to be ready (you'll see "Kafka is ready" in the logs). The default broker URL is `localhost:9092`.

**Keep this terminal running** and open a new terminal for the next steps.

### Step 3: Verify Environment Configuration

Check your `.env` file has the required credentials:

```bash
cd ~/code/crypto-daytrading-arena

# Check environment variables
cat .env | grep -E "TOPSTEPX|OPENAI"
```

You should see:
- `TOPSTEPX_USERNAME`
- `TOPSTEPX_API_KEY`
- `TOPSTEPX_JWT_TOKEN`
- `TOPSTEPX_SYMBOLS` (e.g., `CON.F.US.MES.H26,CON.F.US.MNQ.H26`)
- `OPENAI_API_KEY`

### Step 4: Start the Market Data Connector (TopstepX → Kafka)

In a new terminal:

```bash
cd ~/code/crypto-daytrading-arena

# Start the unified market connector with TopstepX
./run.sh python unified_market_connector.py \
  --provider topstepx \
  --symbols CON.F.US.MES.H26,CON.F.US.MNQ.H26 \
  --bootstrap-servers localhost:9092 \
  --interval 5
```

**What this does:**
- Connects to TopstepX SignalR hub
- Subscribes to market data for Micro E-mini S&P 500 (MES) and NASDAQ (MNQ)
- Publishes tick data to Kafka every 5 seconds
- Agents will receive: current price, bid/ask spread, volume, and recent candles

**Keep this terminal running.**

### Step 5: Deploy Tools & Dashboard

In a new terminal:

```bash
cd ~/code/crypto-daytrading-arena

./run.sh python tools_and_dashboard.py \
  --bootstrap-servers localhost:9092
```

**What this does:**
- Deploys trading tools (execute_trade, get_portfolio, calculator)
- Starts the web dashboard at http://localhost:8501
- Tracks all agent positions, P&L, and trade history

**Keep this terminal running** and you can open http://localhost:8501 in your browser.

### Step 6: Deploy a ChatNode (LLM Inference)

In a new terminal:

```bash
cd ~/code/crypto-daytrading-arena

# Deploy ChatNode using GPT-5 Nano (fast, efficient, powerful)
./run.sh python deploy_chat_node.py \
  --name gpt5-nano \
  --model-id gpt-5-nano \
  --bootstrap-servers localhost:9092 \
  --api-key $OPENAI_API_KEY
```

**Alternative models:**
```bash
# GPT-4o (previous generation flagship)
./run.sh python deploy_chat_node.py \
  --name gpt4o \
  --model-id gpt-4o \
  --bootstrap-servers localhost:9092 \
  --api-key $OPENAI_API_KEY

# GPT-4o-mini (budget-friendly option)
./run.sh python deploy_chat_node.py \
  --name gpt4o-mini \
  --model-id gpt-4o-mini \
  --bootstrap-servers localhost:9092 \
  --api-key $OPENAI_API_KEY

# GPT-4 Turbo
./run.sh python deploy_chat_node.py \
  --name gpt4-turbo \
  --model-id gpt-4-turbo \
  --bootstrap-servers localhost:9092 \
  --api-key $OPENAI_API_KEY
```

**Keep this terminal running.**

### Step 7: Deploy Agent Router(s)

In a new terminal for each agent:

```bash
cd ~/code/crypto-daytrading-arena

# Agent 1: Momentum Trader
./run.sh python deploy_router_node.py \
  --name MomentumTrader \
  --chat-node-name gpt5-nano \
  --strategy momentum \
  --bootstrap-servers localhost:9092

# (Optional) Agent 2: Mean Reversion Trader
# Open another terminal and run:
./run.sh python deploy_router_node.py \
  --name MeanReversionTrader \
  --chat-node-name gpt5-nano \
  --strategy mean_reversion \
  --bootstrap-servers localhost:9092

# (Optional) Agent 3: Contrarian Trader
# Open another terminal and run:
./run.sh python deploy_router_node.py \
  --name ContrarianTrader \
  --chat-node-name gpt5-nano \
  --strategy contrarian \
  --bootstrap-servers localhost:9092
```

**What this does:**
- Each agent receives live market data stream
- Agents analyze prices, spreads, volume, and candles
- Agents execute trades based on their strategy
- All activity appears on the dashboard

**Keep these terminals running.**

### Step 8: (Optional) Start Response Viewer

In a new terminal:

```bash
cd ~/code/crypto-daytrading-arena

./run.sh python response_viewer.py \
  --bootstrap-servers localhost:9092
```

**What this shows:**
- Live stream of agent reasoning
- Tool calls and results
- Agent text responses
- Useful for debugging and understanding agent behavior

## Quick Start Script

For convenience, here's a script that starts everything:

```bash
#!/bin/bash
# save as: start_futures_trading.sh

set -e

echo "🚀 Starting Futures Trading System..."

# Check Docker
if ! docker ps &> /dev/null; then
    echo "❌ Docker is not running. Please start Docker Desktop."
    exit 1
fi

# Check Kafka broker
if ! nc -z localhost 9092 2>/dev/null; then
    echo "⚠️  Kafka broker not running on localhost:9092"
    echo "Starting calfkit-broker..."
    
    if [ ! -d "../calfkit-broker" ]; then
        cd ..
        git clone https://github.com/calf-ai/calfkit-broker
        cd calfkit-broker
    else
        cd ../calfkit-broker
    fi
    
    make dev-up &
    cd ../crypto-daytrading-arena
    
    echo "Waiting for Kafka to be ready..."
    sleep 10
fi

# Start components in background with logging
echo "📊 Starting Market Data Connector..."
./run.sh python unified_market_connector.py \
    --provider topstepx \
    --symbols CON.F.US.MES.H26,CON.F.US.MNQ.H26 \
    --bootstrap-servers localhost:9092 \
    --interval 5 \
    > logs/connector.log 2>&1 &

sleep 3

echo "🛠️  Starting Tools & Dashboard..."
./run.sh python tools_and_dashboard.py \
    --bootstrap-servers localhost:9092 \
    > logs/dashboard.log 2>&1 &

sleep 3

echo "🤖 Deploying ChatNode..."
./run.sh python deploy_chat_node.py \
    --name gpt5-nano \
    --model-id gpt-5-nano \
    --bootstrap-servers localhost:9092 \
    --api-key $OPENAI_API_KEY \
    > logs/chatnode.log 2>&1 &

sleep 3

echo "🎯 Deploying Agent Router..."
./run.sh python deploy_router_node.py \
    --name FuturesTrader \
    --chat-node-name gpt5-nano \
    --strategy momentum \
    --bootstrap-servers localhost:9092 \
    > logs/agent.log 2>&1 &

sleep 3

echo ""
echo "✅ System started!"
echo ""
echo "📊 Dashboard: http://localhost:8501"
echo "📁 Logs: ./logs/"
echo ""
echo "To stop: pkill -f 'python.*deploy_|python.*unified_|python.*tools_'"
```

Make it executable:

```bash
chmod +x start_futures_trading.sh
mkdir -p logs
./start_futures_trading.sh
```

## Monitoring

### Check System Status

```bash
# Check all Python processes
ps aux | grep python | grep -E "deploy_|unified_|tools_"

# Check Kafka topics
docker exec -it calfkit-broker kafka-topics --list --bootstrap-server localhost:9092

# Check logs
tail -f logs/connector.log
tail -f logs/dashboard.log
tail -f logs/agent.log
```

### Dashboard

Open http://localhost:8501 to see:
- Live agent positions
- P&L tracking
- Trade history
- Account balances
- Performance metrics

## Stopping the System

```bash
# Stop all components
pkill -f 'python.*deploy_'
pkill -f 'python.*unified_'
pkill -f 'python.*tools_'

# Stop Kafka broker (if needed)
cd ../calfkit-broker && make dev-down
```

## Troubleshooting

### No Market Data Arriving

```bash
# Check TopstepX connection
./run.sh python topstepx_tick_viewer.py

# Verify Kafka topics
docker exec -it calfkit-broker kafka-topics --list --bootstrap-server localhost:9092
```

### Agents Not Trading

1. Check the response viewer to see agent reasoning
2. Verify ChatNode is running and connected
3. Check agent logs for errors
4. Ensure market data is flowing (check connector logs)

### Dashboard Not Showing Data

1. Ensure tools_and_dashboard.py is running
2. Check http://localhost:8501
3. Refresh the page
4. Check dashboard logs

## Configuration

### Adjust Trading Parameters

Edit `trading_tools.py`:

```python
INITIAL_CASH = 100_000.0  # Starting balance per agent
```

### Change Market Data Update Frequency

```bash
# Update every 10 seconds instead of 5
--interval 10
```

### Add More Contracts

```bash
# Add Micro E-mini Dow Jones
--symbols CON.F.US.MES.H26,CON.F.US.MNQ.H26,CON.F.US.MYM.H26
```

### Use Different LLM Models

```bash
# Claude (if you have Anthropic API key)
./run.sh python deploy_chat_node.py \
  --name claude \
  --model-id claude-3-5-sonnet-20241022 \
  --base-url https://api.anthropic.com \
  --api-key $ANTHROPIC_API_KEY
```

## Next Steps

1. **Monitor agent behavior** via the dashboard and response viewer
2. **Adjust strategies** in `deploy_router_node.py`
3. **Add more agents** with different strategies
4. **Experiment with different models** (GPT-4o vs GPT-4o-mini vs Claude)
5. **Track performance** and optimize agent prompts

## Important Notes

- **This is for educational/testing purposes** - Use simulated TopstepX accounts
- **Market hours matter** - CME futures trade Sun 5pm - Fri 4pm CT
- **Rate limits** - Be mindful of OpenAI API rate limits
- **Costs** - Monitor your OpenAI API usage
- **Data quality** - TopstepX provides real-time CME data during market hours

## Questions?

- Check logs in `./logs/` directory
- Review `TOPSTEPX_QUICKSTART.md` for TopstepX-specific issues
- See `ADAPTER_README.md` for market data adapter details
- Consult `README.md` for general architecture questions
