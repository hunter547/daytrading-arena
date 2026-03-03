# Launch Commands - Quick Reference Card

## 🚀 One-Command Start (Recommended)

```bash
./start_futures_trading.sh
```

Opens dashboard at http://localhost:8501

---

## 📋 Manual Launch (Step-by-Step)

### 1. Start Kafka Broker
```bash
cd ../calfkit-broker && make dev-up
```

### 2. Start Market Data Connector
```bash
./run.sh python unified_market_connector.py \
  --provider topstepx \
  --symbols CON.F.US.MES.H26,CON.F.US.MNQ.H26 \
  --bootstrap-servers localhost:9092 \
  --interval 5
```

### 3. Start Dashboard & Tools
```bash
./run.sh python tools_and_dashboard.py \
  --bootstrap-servers localhost:9092
```

### 4. Deploy ChatNode (GPT-5 Nano)
```bash
./run.sh python deploy_chat_node.py \
  --name gpt5-nano \
  --model-id gpt-5-nano \
  --bootstrap-servers localhost:9092 \
  --api-key $OPENAI_API_KEY
```

### 5. Deploy Agent
```bash
./run.sh python deploy_router_node.py \
  --name FuturesTrader \
  --chat-node-name gpt5-nano \
  --strategy momentum \
  --bootstrap-servers localhost:9092
```

---

## 🛑 Stop Commands

```bash
# Stop all components
./stop_futures_trading.sh

# Or manually
pkill -f 'python.*deploy_|python.*unified_|python.*tools_'

# Stop Kafka (optional)
cd ../calfkit-broker && make dev-down
```

---

## 🔧 Testing Commands

```bash
# Test TopstepX connection
./run.sh python topstepx_tick_viewer.py

# Test TopstepX User Hub
./run.sh python test_topstepx_user_hub.py

# Debug SignalR connection
./run.sh python debug_topstepx_signalr.py

# Authenticate and get JWT token
./run.sh python topstepx_auth.py
```

---

## 📊 Monitoring Commands

```bash
# View all logs
tail -f logs/*.log

# Agent reasoning
tail -f logs/agent.log

# Market data
tail -f logs/connector.log

# Dashboard
tail -f logs/dashboard.log

# ChatNode (GPT-5 Nano)
tail -f logs/chatnode.log

# Check running processes
ps aux | grep python | grep -E "deploy_|unified_|tools_"

# Check Kafka topics
docker exec -it calfkit-broker kafka-topics --list --bootstrap-server localhost:9092
```

---

## 🎯 Deploy Multiple Agents

```bash
# Agent 1 (Momentum)
./run.sh python deploy_router_node.py \
  --name Momentum --chat-node-name gpt5-nano \
  --strategy momentum --bootstrap-servers localhost:9092

# Agent 2 (Mean Reversion)
./run.sh python deploy_router_node.py \
  --name MeanReversion --chat-node-name gpt5-nano \
  --strategy mean_reversion --bootstrap-servers localhost:9092

# Agent 3 (Contrarian)
./run.sh python deploy_router_node.py \
  --name Contrarian --chat-node-name gpt5-nano \
  --strategy contrarian --bootstrap-servers localhost:9092
```

---

## 🔀 Alternative Models

```bash
# GPT-4o
./run.sh python deploy_chat_node.py \
  --name gpt4o --model-id gpt-4o \
  --bootstrap-servers localhost:9092 --api-key $OPENAI_API_KEY

# GPT-4o-mini
./run.sh python deploy_chat_node.py \
  --name gpt4o-mini --model-id gpt-4o-mini \
  --bootstrap-servers localhost:9092 --api-key $OPENAI_API_KEY

# Then point agents to it:
./run.sh python deploy_router_node.py \
  --name Agent --chat-node-name gpt4o \
  --strategy momentum --bootstrap-servers localhost:9092
```

---

## 📈 Environment Customization

```bash
# Use different symbols
TOPSTEPX_SYMBOLS=CON.F.US.MES.H26,CON.F.US.MNQ.H26,CON.F.US.MYM.H26 \
  ./start_futures_trading.sh

# Use different model
CHAT_MODEL=gpt-4o CHAT_NODE_NAME=gpt4o \
  ./start_futures_trading.sh

# Change update frequency
INTERVAL=10 ./start_futures_trading.sh

# Combine multiple options
INTERVAL=10 CHAT_MODEL=gpt-4o STRATEGY=mean_reversion \
  ./start_futures_trading.sh
```

---

## 🐛 Debugging

```bash
# Check Docker
docker ps

# Test Kafka connection
nc -z localhost 9092 && echo "Kafka OK" || echo "Kafka NOT running"

# Check environment
cat .env | grep -E "TOPSTEPX|OPENAI"

# Verify contracts are correct
./run.sh python -c "from dotenv import load_dotenv; import os; load_dotenv(); print(os.getenv('TOPSTEPX_SYMBOLS'))"

# List Kafka topics
docker exec -it calfkit-broker kafka-topics --bootstrap-server localhost:9092 --list

# Check if market data is flowing
docker exec -it calfkit-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic market_data.prices \
  --from-beginning --max-messages 5
```

---

## 📱 URLs

- **Dashboard**: http://localhost:8501
- **Kafka Broker**: localhost:9092
- **TopstepX API**: https://api.topstepx.com
- **TopstepX WebSocket**: https://rtc.topstepx.com/hubs/market

---

## ⚡ Quick Restart

```bash
./stop_futures_trading.sh && sleep 2 && ./start_futures_trading.sh
```

---

## 💾 Save This File!

Keep this handy for quick command reference. All commands assume you're in the project root directory:
```bash
cd ~/code/crypto-daytrading-arena
```
