#!/bin/bash

# Quick Start Script for AI Trading on TopstepX
# This script helps you launch all required services in order

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo ""
echo "================================================================"
echo "  AI Trading Setup for TopstepX Practice Account"
echo "================================================================"
echo ""

# Check prerequisites
echo "Checking prerequisites..."
echo ""

# Check .env file
if [ ! -f ".env" ]; then
    echo -e "${RED}❌ Error: .env file not found${NC}"
    echo "   Run: ./setup_env.sh"
    exit 1
fi

# Load .env
export $(grep -v '^#' .env | xargs)

# Check TopstepX token
if [ -z "$TOPSTEPX_JWT_TOKEN" ]; then
    echo -e "${RED}❌ Error: TOPSTEPX_JWT_TOKEN not set in .env${NC}"
    echo "   Run: ./run.sh python topstepx_auth.py"
    exit 1
fi
echo -e "${GREEN}✓ TopstepX token found${NC}"

# Check OpenAI key
if [ -z "$OPENAI_API_KEY" ]; then
    echo -e "${RED}❌ Error: OPENAI_API_KEY not set in .env${NC}"
    echo "   Add your OpenAI API key to .env file"
    exit 1
fi
echo -e "${GREEN}✓ OpenAI API key found${NC}"

# Check Kafka
if ! docker ps | grep -q kafka; then
    echo -e "${YELLOW}⚠  Kafka not running - attempting to start...${NC}"
    docker-compose up -d kafka
    sleep 3
fi
echo -e "${GREEN}✓ Kafka is running${NC}"

echo ""
echo "================================================================"
echo "  All prerequisites met! Ready to start AI trading."
echo "================================================================"
echo ""
echo "You need to run 3-4 services in separate terminals:"
echo ""
echo -e "${BLUE}Terminal 1 - Trading Tools Service (Required):${NC}"
echo "  ./run.sh python topstepx_trading_tools.py --bootstrap-servers localhost:9092"
echo ""
echo -e "${BLUE}Terminal 2 - Market Data Feed (Recommended):${NC}"
echo "  ./run.sh python unified_market_connector.py \\"
echo "    --provider topstepx \\"
echo "    --symbols CON.F.US.MES.H26 CON.F.US.MNQ.H26 \\"
echo "    --bootstrap-servers localhost:9092 --interval 5"
echo ""
echo -e "${BLUE}Terminal 3 - AI Trading Agent:${NC}"
echo "  ./run.sh python ai_trader.py --bootstrap-servers localhost:9092"
echo ""
echo -e "${BLUE}Terminal 4 - Dashboard (Optional):${NC}"
echo "  ./run.sh python tools_and_dashboard.py --bootstrap-servers localhost:9092"
echo ""
echo "================================================================"
echo ""
echo -e "${YELLOW}TIP: Start with advisor mode first to watch without trading:${NC}"
echo "  ./run.sh python ai_trader.py --bootstrap-servers localhost:9092 --advisor-mode"
echo ""
echo "================================================================"
echo ""

# Ask if user wants to start services automatically
read -p "Do you want to start services now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo ""
    echo "Starting services in background..."
    echo ""
    
    # Start trading tools
    echo "Starting TopstepX trading tools..."
    ./run.sh python topstepx_trading_tools.py --bootstrap-servers localhost:9092 > logs/trading_tools.log 2>&1 &
    TOOLS_PID=$!
    sleep 2
    
    # Start market data
    echo "Starting market data feed..."
    ./run.sh python unified_market_connector.py \
      --provider topstepx \
      --symbols CON.F.US.MES.H26 CON.F.US.MNQ.H26 \
      --bootstrap-servers localhost:9092 \
      --interval 5 > logs/market_data.log 2>&1 &
    MARKET_PID=$!
    sleep 2
    
    # Start dashboard
    echo "Starting dashboard..."
    ./run.sh python tools_and_dashboard.py --bootstrap-servers localhost:9092 > logs/dashboard.log 2>&1 &
    DASH_PID=$!
    sleep 2
    
    echo ""
    echo -e "${GREEN}✓ Services started!${NC}"
    echo ""
    echo "Process IDs:"
    echo "  Trading Tools: $TOOLS_PID"
    echo "  Market Data: $MARKET_PID"
    echo "  Dashboard: $DASH_PID"
    echo ""
    echo "Logs available in:"
    echo "  logs/trading_tools.log"
    echo "  logs/market_data.log"
    echo "  logs/dashboard.log"
    echo ""
    echo "To start the AI agent, run:"
    echo -e "${BLUE}  ./run.sh python ai_trader.py --bootstrap-servers localhost:9092${NC}"
    echo ""
    echo "To stop all services, run:"
    echo -e "${BLUE}  kill $TOOLS_PID $MARKET_PID $DASH_PID${NC}"
    echo ""
fi
