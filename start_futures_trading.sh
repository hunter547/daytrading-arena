#!/bin/bash
# Futures Trading System Startup Script
# This script starts all components needed for multi-agent futures trading with TopstepX

set -e

echo ""
echo "🚀 Starting Futures Trading System with TopstepX"
echo "=================================================="
echo ""

# Create logs directory
mkdir -p logs

# Check Docker
echo "Checking Docker..."
if ! docker ps &> /dev/null; then
    echo "❌ Docker is not running. Please start Docker Desktop first."
    echo ""
    echo "To start:"
    echo "  - macOS: Open Docker Desktop from Applications"
    echo "  - Linux: sudo systemctl start docker"
    echo ""
    exit 1
fi
echo "✓ Docker is running"
echo ""

# Check/Start Kafka broker
echo "Checking Kafka broker..."
if ! nc -z localhost 9092 2>/dev/null; then
    echo "⚠️  Kafka broker not detected on localhost:9092"
    
    # Check if calfkit-broker exists
    if [ ! -d "../calfkit-broker" ]; then
        echo "Cloning calfkit-broker repository..."
        (
            cd ..
            git clone https://github.com/calf-ai/calfkit-broker
        )
    fi
    
    echo "Starting Kafka broker (this may take a minute)..."
    (
        cd ../calfkit-broker
        make dev-up > ../crypto-daytrading-arena/logs/kafka-broker.log 2>&1 &
    )
    
    echo "Waiting for Kafka to be ready..."
    for i in {1..30}; do
        if nc -z localhost 9092 2>/dev/null; then
            echo "✓ Kafka broker ready!"
            break
        fi
        echo -n "."
        sleep 2
    done
    
    if ! nc -z localhost 9092 2>/dev/null; then
        echo ""
        echo "❌ Kafka broker failed to start after 60s"
        echo "Check logs: tail -f logs/kafka-broker.log"
        exit 1
    fi
else
    echo "✓ Kafka broker already running"
fi
echo ""

# Check environment
echo "Checking environment configuration..."
if [ ! -f .env ]; then
    echo "❌ .env file not found"
    echo "Please create .env file with:"
    echo "  - TOPSTEPX_USERNAME"
    echo "  - TOPSTEPX_API_KEY"
    echo "  - TOPSTEPX_SYMBOLS"
    echo "  - OPENAI_API_KEY"
    exit 1
fi

source .env

if [ -z "$TOPSTEPX_USERNAME" ] || [ -z "$TOPSTEPX_API_KEY" ]; then
    echo "❌ TopstepX credentials not configured in .env"
    exit 1
fi

if [ -z "$OPENAI_API_KEY" ]; then
    echo "❌ OPENAI_API_KEY not configured in .env"
    exit 1
fi

SYMBOLS="${TOPSTEPX_SYMBOLS:-CON.F.US.MES.H26,CON.F.US.MNQ.H26}"
echo "✓ Environment configured"
echo "  - Symbols: $SYMBOLS"
echo ""

# Start components
BROKER_URL="localhost:9092"
INTERVAL="${INTERVAL:-5}"

echo "=================================================="
echo "Starting System Components"
echo "=================================================="
echo ""

# 1. Market Data Connector
echo "1️⃣  Starting Market Data Connector (TopstepX → Kafka)..."
./run.sh python unified_market_connector.py \
    --provider topstepx \
    --symbols "$SYMBOLS" \
    --bootstrap-servers "$BROKER_URL" \
    --interval "$INTERVAL" \
    > logs/connector.log 2>&1 &
CONNECTOR_PID=$!
echo "   PID: $CONNECTOR_PID"
echo "   Log: logs/connector.log"
sleep 3
echo ""

# 2. Tools & Dashboard
echo "2️⃣  Starting Tools & Dashboard..."
./run.sh python tools_and_dashboard.py \
    --bootstrap-servers "$BROKER_URL" \
    > logs/dashboard.log 2>&1 &
DASHBOARD_PID=$!
echo "   PID: $DASHBOARD_PID"
echo "   Log: logs/dashboard.log"
echo "   URL: http://localhost:8501"
sleep 3
echo ""

# 3. ChatNode
CHAT_MODEL="${CHAT_MODEL:-gpt-5-nano}"
CHAT_NODE_NAME="${CHAT_NODE_NAME:-gpt5-nano}"

echo "3️⃣  Deploying ChatNode ($CHAT_MODEL)..."
./run.sh python deploy_chat_node.py \
    --name "$CHAT_NODE_NAME" \
    --model-id "$CHAT_MODEL" \
    --bootstrap-servers "$BROKER_URL" \
    --api-key "$OPENAI_API_KEY" \
    > logs/chatnode.log 2>&1 &
CHATNODE_PID=$!
echo "   PID: $CHATNODE_PID"
echo "   Log: logs/chatnode.log"
sleep 3
echo ""

# 4. Agent Router
AGENT_NAME="${AGENT_NAME:-FuturesTrader}"
STRATEGY="${STRATEGY:-momentum}"

echo "4️⃣  Deploying Agent Router ($AGENT_NAME - $STRATEGY strategy)..."
./run.sh python deploy_router_node.py \
    --name "$AGENT_NAME" \
    --chat-node-name "$CHAT_NODE_NAME" \
    --strategy "$STRATEGY" \
    --bootstrap-servers "$BROKER_URL" \
    > logs/agent.log 2>&1 &
AGENT_PID=$!
echo "   PID: $AGENT_PID"
echo "   Log: logs/agent.log"
sleep 3
echo ""

# 5. (Optional) Response Viewer
if [ "$START_VIEWER" = "true" ]; then
    echo "5️⃣  Starting Response Viewer..."
    ./run.sh python response_viewer.py \
        --bootstrap-servers "$BROKER_URL" \
        > logs/viewer.log 2>&1 &
    VIEWER_PID=$!
    echo "   PID: $VIEWER_PID"
    echo "   Log: logs/viewer.log"
    echo ""
fi

# Summary
echo "=================================================="
echo "✅ System Started Successfully!"
echo "=================================================="
echo ""
echo "📊 Dashboard:    http://localhost:8501"
echo "📁 Logs:         ./logs/"
echo "🔧 Components:"
echo "   - Connector:  PID $CONNECTOR_PID (logs/connector.log)"
echo "   - Dashboard:  PID $DASHBOARD_PID (logs/dashboard.log)"
echo "   - ChatNode:   PID $CHATNODE_PID (logs/chatnode.log)"
echo "   - Agent:      PID $AGENT_PID (logs/agent.log)"
if [ "$START_VIEWER" = "true" ]; then
    echo "   - Viewer:     PID $VIEWER_PID (logs/viewer.log)"
fi
echo ""
echo "💡 Tips:"
echo "   - Open dashboard in browser to see agent activity"
echo "   - tail -f logs/agent.log to see agent reasoning"
echo "   - tail -f logs/connector.log to see market data"
echo ""
echo "🛑 To stop all components:"
echo "   pkill -f 'python.*deploy_|python.*unified_|python.*tools_'"
echo "   or press Ctrl+C and run: ./stop_futures_trading.sh"
echo ""
echo "=================================================="
echo ""

# Save PIDs for cleanup
echo "$CONNECTOR_PID" > logs/pids.txt
echo "$DASHBOARD_PID" >> logs/pids.txt
echo "$CHATNODE_PID" >> logs/pids.txt
echo "$AGENT_PID" >> logs/pids.txt
if [ "$START_VIEWER" = "true" ]; then
    echo "$VIEWER_PID" >> logs/pids.txt
fi

echo "System is running. Press Ctrl+C to stop all components."
echo ""

# Wait for Ctrl+C
trap 'echo ""; echo "Stopping all components..."; kill $(cat logs/pids.txt) 2>/dev/null; rm -f logs/pids.txt; echo "✅ Stopped"; exit 0' INT TERM

# Keep script running
tail -f /dev/null
