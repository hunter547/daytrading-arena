# Quick Start Guide

Get up and running with the market data adapter system in under 5 minutes.

## 1. Setup (One-time)

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install --upgrade pip
pip install -e .
pip install signalrcore  # For TopstepX support
```

## 2. Choose Your Provider

### Option A: Coinbase (Cryptocurrency)

```bash
# Activate virtual environment
source venv/bin/activate

# Run connector
python unified_market_connector.py \
    --provider coinbase \
    --symbols BTC-USD ETH-USD SOL-USD \
    --interval 5
```

**That's it!** No configuration needed. You'll see real-time crypto prices streaming.

### Option B: TopstepX (CME Futures)

**First, get your JWT token from your API key:**

```bash
# Activate virtual environment
source venv/bin/activate

# Get JWT token (run once, valid 24 hours)
python topstepx_auth.py \
    --username YOUR_USERNAME \
    --api-key YOUR_API_KEY \
    --environment demo

# Copy the token from output and set it
export TOPSTEPX_JWT_TOKEN="your_jwt_token_from_above"

# Run connector
python unified_market_connector.py \
    --provider topstepx \
    --symbols CON.F.US.ES.H26 CON.F.US.NQ.H26 \
    --interval 1
```

**Or use automatic authentication (even easier):**

```bash
# Set your credentials (no need to get token manually)
export TOPSTEPX_USERNAME=your_username
export TOPSTEPX_API_KEY=your_api_key
export TOPSTEPX_ENVIRONMENT=demo

# Run connector (it will auto-authenticate)
python unified_market_connector.py \
    --provider topstepx \
    --symbols CON.F.US.ES.H26 CON.F.US.NQ.H26 \
    --interval 1
```

**Common TopstepX Contract IDs:**
- `CON.F.US.ES.H26` - E-mini S&P 500 March 2025
- `CON.F.US.NQ.H26` - E-mini NASDAQ March 2025
- `CON.F.US.RTY.H26` - E-mini Russell 2000 March 2025
- `CON.F.US.YM.H26` - E-mini Dow March 2025

## 3. Test It Out

```bash
# Quick test (no Kafka required)
source venv/bin/activate
python example_adapter_usage.py --example coinbase
```

You should see live quotes streaming to your terminal!

## Command Reference

### Unified Market Connector

```bash
python unified_market_connector.py \
    --provider {coinbase|topstepx} \
    --symbols SYMBOL1 SYMBOL2 ... \
    [--interval SECONDS] \
    [--candle-interval SECONDS] \
    [--router-name NAME]
```

**Options:**
- `--provider` - Data provider: `coinbase` or `topstepx` (required)
- `--symbols` - Space-separated list of symbols (required)
- `--interval` - Minimum seconds between publishes per symbol (default: 0)
- `--candle-interval` - Candle refresh interval in seconds (default: 60)
- `--router-name` - Router node name (default: default)

### Examples

```bash
# Examples (run one at a time)
python example_adapter_usage.py --example coinbase    # Test Coinbase
python example_adapter_usage.py --example topstepx    # Test TopstepX
python example_adapter_usage.py --example comparison  # Compare both
python example_adapter_usage.py --example all         # Run all examples
```

## Environment Variables

### Kafka (for full trading system)

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

### TopstepX

**Option 1: Use API key (automatic authentication)**
```bash
export TOPSTEPX_USERNAME=your_username
export TOPSTEPX_API_KEY=your_api_key
export TOPSTEPX_ENVIRONMENT=demo  # or topstepx, alpha-ticks, etc.
```

**Option 2: Use JWT token directly (if you already have one)**
```bash
export TOPSTEPX_JWT_TOKEN=your_jwt_token_here
export TOPSTEPX_ENVIRONMENT=demo
```

**To get a JWT token from your API key:**
```bash
python topstepx_auth.py --username YOUR_USER --api-key YOUR_KEY
```

See `TOPSTEPX_AUTH_GUIDE.md` for detailed authentication instructions.

## Switching Providers

To switch from Coinbase to TopstepX (or vice versa):

**Just change the command!** No code changes needed.

```bash
# From Coinbase:
python unified_market_connector.py --provider coinbase --symbols BTC-USD

# To TopstepX:
export TOPSTEPX_JWT_TOKEN=your_token
python unified_market_connector.py --provider topstepx --symbols CON.F.US.ES.H26
```

Your agents, tools, and dashboard continue working unchanged!

## Full Trading System (with Agents)

```bash
# Activate environment
source venv/bin/activate

# Set environment
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export OPENAI_API_KEY=your_openai_key

# Terminal 1: Chat node (LLM)
python deploy_chat_node.py

# Terminal 2: Router node (agents)
python deploy_router_node.py --strategy momentum --name agent1

# Terminal 3: Market data
python unified_market_connector.py --provider coinbase --symbols BTC-USD ETH-USD

# Terminal 4: Dashboard
python tools_and_dashboard.py

# Terminal 5: Response viewer
python response_viewer.py
```

## Troubleshooting

### Virtual environment not activated?

You should see `(venv)` in your terminal prompt. If not:

```bash
source venv/bin/activate  # Linux/macOS
venv\Scripts\activate     # Windows
```

### Import errors?

```bash
# Reinstall dependencies
source venv/bin/activate
pip install -e .
```

### TopstepX: signalrcore not found?

```bash
source venv/bin/activate
pip install signalrcore
```

### No data streaming?

**Coinbase:**
- Check internet connection
- Verify symbols are correct (e.g., `BTC-USD` not `BTCUSD`)

**TopstepX:**
- Verify `TOPSTEPX_JWT_TOKEN` is set: `echo $TOPSTEPX_JWT_TOKEN`
- Check token hasn't expired
- Ensure contract IDs are current (e.g., `H26` for March 2025)

## What's Next?

1. **Read full documentation:**
   - `ADAPTER_README.md` - Complete adapter system guide
   - `MIGRATION_SUMMARY.md` - Detailed comparison and migration

2. **Add your API keys:**
   - Create `.env` file with `OPENAI_API_KEY=your_key`
   - Set `TOPSTEPX_JWT_TOKEN` if using futures

3. **Run the full system:**
   - Start Kafka broker
   - Deploy chat and router nodes
   - Launch market connector
   - Monitor dashboard

4. **Customize strategies:**
   - Edit `deploy_router_node.py` for custom trading logic
   - Modify `trading_tools.py` for new tools
   - Add more symbols to track

## Summary

**Three commands to get started:**

```bash
# 1. Setup (one-time)
python3 -m venv venv && source venv/bin/activate && pip install -e . && pip install signalrcore

# 2. Run with Coinbase
python unified_market_connector.py --provider coinbase --symbols BTC-USD

# 3. Or run with TopstepX
export TOPSTEPX_JWT_TOKEN=your_token && python unified_market_connector.py --provider topstepx --symbols CON.F.US.ES.H26
```

**You're ready to trade!** 🚀
