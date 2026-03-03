# Setup Guide

This guide walks you through setting up the crypto-daytrading-arena project with the new market data adapter system.

## Prerequisites

- Python 3.10 or higher (tested with Python 3.12.3)
- Git
- (Optional) Kafka broker for running the full system

## Installation Steps

### 1. Clone the Repository

```bash
git clone <repository-url>
cd crypto-daytrading-arena
```

### 2. Create Virtual Environment

```bash
python3 -m venv venv
```

### 3. Activate Virtual Environment

**On Linux/macOS:**
```bash
source venv/bin/activate
```

**On Windows:**
```bash
venv\Scripts\activate
```

### 4. Upgrade pip

```bash
pip install --upgrade pip
```

### 5. Install Project Dependencies

```bash
# Install in editable mode with all dependencies
pip install -e .
```

This will install:
- calfkit (agent framework)
- httpx (HTTP client)
- websockets (WebSocket client for Coinbase)
- pydantic (data validation)
- rich (terminal UI)
- plotext (plotting)
- sympy (symbolic math)
- python-dotenv (environment variables)
- And all their dependencies

### 6. Install TopstepX Support (Optional)

If you plan to use TopstepX for CME futures:

```bash
pip install signalrcore
```

Or install via the optional dependency group:

```bash
pip install -e ".[topstepx]"
```

### 7. Verify Installation

```bash
# Check all packages installed
pip list | grep -E "(calfkit|websockets|httpx|pydantic|signalrcore)"

# Test imports
python -c "import market_data_adapter; import coinbase_adapter; import topstepx_adapter; print('✓ All modules loaded')"
```

You should see:
```
✓ All modules loaded
```

## Environment Configuration

### For Coinbase (Crypto)

No additional configuration needed! Just run:

```bash
python unified_market_connector.py \
    --provider coinbase \
    --symbols BTC-USD ETH-USD SOL-USD
```

### For TopstepX (CME Futures)

1. **Get your JWT token** from TopstepX Gateway
2. **Create `.env` file** (or set environment variables):

```bash
# .env file
TOPSTEPX_JWT_TOKEN=your_jwt_token_here
TOPSTEPX_ENVIRONMENT=demo  # or topstepx, alpha-ticks, etc.
```

3. **Run connector:**

```bash
source .env  # Or: export TOPSTEPX_JWT_TOKEN=your_token
python unified_market_connector.py \
    --provider topstepx \
    --symbols CON.F.US.ES.H26 CON.F.US.NQ.H26
```

### For Full Trading System (with Kafka)

1. **Start Kafka broker** (if not running):

```bash
# Using Docker
docker run -d \
    --name kafka \
    -p 9092:9092 \
    -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
    apache/kafka:latest

# Or use your existing Kafka setup
```

2. **Set Kafka configuration:**

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

3. **Run the full system:**

```bash
# Terminal 1: Start chat node (LLM inference)
python deploy_chat_node.py

# Terminal 2: Start router node (agent orchestration)
python deploy_router_node.py --strategy momentum

# Terminal 3: Start market data connector
python unified_market_connector.py \
    --provider coinbase \
    --symbols BTC-USD ETH-USD

# Terminal 4: Monitor dashboard
python tools_and_dashboard.py

# Terminal 5: View agent responses
python response_viewer.py
```

## Quick Test

Run the example script to verify everything works:

```bash
# Test Coinbase adapter only (no Kafka required)
python example_adapter_usage.py --example coinbase

# Test both adapters (requires TopstepX token)
export TOPSTEPX_JWT_TOKEN=your_token
python example_adapter_usage.py --example all
```

## Directory Structure

```
crypto-daytrading-arena/
├── venv/                           # Virtual environment (created)
├── market_data_adapter.py          # Abstract adapter interface
├── coinbase_adapter.py             # Coinbase implementation
├── topstepx_adapter.py             # TopstepX implementation
├── unified_market_connector.py     # Unified connector
├── example_adapter_usage.py        # Usage examples
├── coinbase_connector.py           # Original Coinbase connector (still works)
├── coinbase_consumer.py            # Original consumer
├── coinbase_kafka_connector.py     # Original Kafka connector
├── trading_tools.py                # Trading tools (execute_trade, etc.)
├── deploy_chat_node.py             # LLM chat node
├── deploy_router_node.py           # Agent router
├── tools_and_dashboard.py          # Dashboard
├── response_viewer.py              # Response viewer
├── ADAPTER_README.md               # Adapter documentation
├── MIGRATION_SUMMARY.md            # Migration guide
├── SETUP_GUIDE.md                  # This file
├── pyproject.toml                  # Project config
├── requirements.txt                # Generated requirements
└── README.md                       # Main README
```

## Troubleshooting

### Import Errors

If you see import errors, make sure:
1. Virtual environment is activated: `source venv/bin/activate`
2. Project installed: `pip install -e .`
3. All dependencies installed: `pip list`

### Module Not Found: signalrcore

For TopstepX support:
```bash
pip install signalrcore
```

### Kafka Connection Errors

Ensure Kafka is running and accessible:
```bash
# Test Kafka connection
nc -zv localhost 9092
```

Set correct broker address:
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

### WebSocket Connection Issues

For Coinbase:
- Check internet connection
- Verify firewall allows WebSocket connections
- Check Coinbase status: https://status.coinbase.com/

For TopstepX:
- Verify JWT token is valid
- Check token expiration
- Confirm environment matches your account

## Next Steps

1. **Read the documentation:**
   - `ADAPTER_README.md` - Comprehensive adapter system docs
   - `MIGRATION_SUMMARY.md` - Migration guide and comparison

2. **Try the examples:**
   ```bash
   python example_adapter_usage.py --example all
   ```

3. **Run with your preferred provider:**
   ```bash
   # Coinbase
   python unified_market_connector.py --provider coinbase --symbols BTC-USD
   
   # TopstepX
   export TOPSTEPX_JWT_TOKEN=your_token
   python unified_market_connector.py --provider topstepx --symbols CON.F.US.ES.H26
   ```

4. **Integrate with trading agents:**
   - Configure strategies in `deploy_router_node.py`
   - Deploy agents to trade on live data
   - Monitor via dashboard

## Support

For issues or questions:
1. Check `ADAPTER_README.md` troubleshooting section
2. Review `MIGRATION_SUMMARY.md` for common scenarios
3. Verify environment configuration
4. Check logs for error messages

## Summary

You now have a fully configured environment with:
- ✅ Virtual environment created and activated
- ✅ All core dependencies installed
- ✅ TopstepX support (SignalR) installed
- ✅ Adapter modules verified and working
- ✅ Ready to run with Coinbase or TopstepX

**Start trading with a single command!**

```bash
# Activate environment
source venv/bin/activate

# Run with your preferred provider
python unified_market_connector.py --provider coinbase --symbols BTC-USD
```
