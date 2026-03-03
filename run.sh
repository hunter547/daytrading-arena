#!/bin/bash

# ============================================================================
# Crypto Day Trading Arena - Runner Script
# ============================================================================
# This script handles virtual environment activation and environment loading
# Usage: ./run.sh [command and arguments]
#
# Examples:
#   ./run.sh python unified_market_connector.py --provider coinbase --symbols BTC-USD
#   ./run.sh python topstepx_auth.py
#   ./run.sh python example_adapter_usage.py --example all

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo -e "${RED}Error: Virtual environment not found!${NC}"
    echo "Please run: python3 -m venv venv && pip install -e ."
    exit 1
fi

# Activate virtual environment
echo -e "${GREEN}✓ Activating virtual environment...${NC}"
source venv/bin/activate

# Load .env if it exists
if [ -f ".env" ]; then
    echo -e "${GREEN}✓ Loading environment from .env...${NC}"
    set -a
    source .env
    set +a
else
    echo -e "${YELLOW}⚠ Warning: .env file not found${NC}"
    echo "  Run './setup_env.sh' to create one"
    echo ""
fi

# Run the command
if [ $# -eq 0 ]; then
    echo "Usage: ./run.sh [command and arguments]"
    echo ""
    echo "Examples:"
    echo "  ./run.sh python unified_market_connector.py --provider coinbase --symbols BTC-USD"
    echo "  ./run.sh python topstepx_auth.py"
    echo "  ./run.sh python example_adapter_usage.py --example all"
    exit 1
fi

echo -e "${GREEN}✓ Running: $@${NC}"
echo ""

# Execute the command
exec "$@"
