# TopstepX Quick Start Guide

Get started with TopstepX CME futures market data in under 5 minutes.

## Official TopstepX Connection URLs

```
API Endpoint:  https://api.topstepx.com
User Hub:      https://rtc.topstepx.com/hubs/user
Market Hub:    https://rtc.topstepx.com/hubs/market
```

## Prerequisites

From your TopstepX account, you need:

1. **Username/Email** - Your TopstepX login email
2. **API Key** - Get this from your TopstepX dashboard

## Setup (5 Minutes)

### Step 1: Create Your .env File

Choose one method:

**Method A: Use the Template (Easiest)**

```bash
# Copy the TopstepX template
cp .env.topstepx-direct .env

# Edit with your credentials
nano .env
```

**Method B: Create Manually**

```bash
cat > .env << 'EOF'
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# OpenAI (optional, for AI agents)
OPENAI_API_KEY=your-openai-key-here

# TopstepX Credentials
TOPSTEPX_USERNAME=your-email@example.com
TOPSTEPX_API_KEY=your-api-key-here

# TopstepX API Settings
TOPSTEPX_ENVIRONMENT=topstepx-direct
TOPSTEPX_API_URL=https://api.topstepx.com
EOF
```

### Step 2: Activate Virtual Environment

```bash
source venv/bin/activate
```

### Step 3: Load Environment Variables

```bash
source .env
```

### Step 4: Test Authentication

```bash
python topstepx_auth.py --environment topstepx-direct
```

**Expected Output:**

```
2026-02-25 17:10:00 [INFO] Authenticating with TopstepX (topstepx-direct)...
2026-02-25 17:10:01 [INFO] ✓ Authentication successful! JWT token obtained.

======================================================================
SUCCESS! Your JWT Token:
======================================================================
eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
======================================================================

To use this token, set it as an environment variable:
export TOPSTEPX_JWT_TOKEN='eyJhbGciOi...'

Or add to your .env file:
TOPSTEPX_JWT_TOKEN=eyJhbGciOi...

Note: Tokens are valid for 24 hours
======================================================================
```

### Step 5: Run Market Data Connector

```bash
python unified_market_connector.py \
    --provider topstepx \
    --symbols CON.F.US.ES.H26 CON.F.US.NQ.H26
```

**You should see:**

```
[INFO] No JWT token found, authenticating with API key...
[INFO] Authenticating with TopstepX (topstepx-direct)...
[INFO] ✓ Authentication successful! JWT token obtained.
[INFO] ✓ Authenticated successfully! Token obtained.
[INFO] Using TopstepX adapter (topstepx-direct) for: ['CON.F.US.ES.H26', 'CON.F.US.NQ.H26']
[INFO] Starting unified market connector
[INFO] TopstepX SignalR connected
[INFO] Subscribed to TopstepX market data: CON.F.US.ES.H26
[INFO] Subscribed to TopstepX market data: CON.F.US.NQ.H26
```

## Using the Helper Script

For even easier usage, use the `run.sh` script:

```bash
# Test authentication
./run.sh python topstepx_auth.py --environment topstepx-direct

# Run market connector
./run.sh python unified_market_connector.py \
    --provider topstepx \
    --symbols CON.F.US.ES.H26
```

The `run.sh` script automatically:
- Activates the virtual environment
- Loads your .env file
- Runs your command

## Common Contract Symbols

### E-mini Futures (CME)

| Contract | Symbol | Description |
|----------|--------|-------------|
| E-mini S&P 500 | `CON.F.US.ES.H26` | March 2025 |
| E-mini NASDAQ-100 | `CON.F.US.NQ.H26` | March 2025 |
| E-mini Russell 2000 | `CON.F.US.RTY.H26` | March 2025 |
| E-mini Dow | `CON.F.US.YM.H26` | March 2025 |

### Contract Month Codes

| Code | Month | Code | Month |
|------|-------|------|-------|
| F | January | U | September |
| G | February | V | October |
| H | March | X | November |
| J | April | Z | December |
| K | May | | |
| M | June | | |
| N | July | | |
| Q | August | | |

**Example:** `CON.F.US.ES.H26` = E-mini S&P 500, March 2025

## Troubleshooting

### Issue: "Authentication failed"

**Check your credentials:**

```bash
echo "Username: $TOPSTEPX_USERNAME"
echo "API Key: $TOPSTEPX_API_KEY"
echo "Environment: $TOPSTEPX_ENVIRONMENT"
echo "API URL: $TOPSTEPX_API_URL"
```

**Verify they match your TopstepX account.**

### Issue: "Name or service not known"

**Check your environment settings:**

```bash
# Should be:
TOPSTEPX_ENVIRONMENT=topstepx-direct
TOPSTEPX_API_URL=https://api.topstepx.com

# NOT:
# TOPSTEPX_ENVIRONMENT=demo  ← Wrong!
```

### Issue: "Token expired"

JWT tokens expire after 24 hours. Re-authenticate:

```bash
./run.sh python topstepx_auth.py --environment topstepx-direct
```

### Issue: "No market data"

**Verify contract symbols are current:**

- Check the contract month (H26 = March 2025)
- Update to active contracts
- Make sure you have market data permissions

## Complete Example

Here's a complete working example from scratch:

```bash
# 1. Navigate to project directory
cd /path/to/crypto-daytrading-arena

# 2. Activate virtual environment
source venv/bin/activate

# 3. Create .env file
cat > .env << 'EOF'
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
TOPSTEPX_USERNAME=john.doe@example.com
TOPSTEPX_API_KEY=abc123xyz789
TOPSTEPX_ENVIRONMENT=topstepx-direct
TOPSTEPX_API_URL=https://api.topstepx.com
EOF

# 4. Load environment
source .env

# 5. Test authentication
python topstepx_auth.py --environment topstepx-direct

# 6. Start market data
python unified_market_connector.py \
    --provider topstepx \
    --symbols CON.F.US.ES.H26 CON.F.US.NQ.H26 \
    --interval 1

# 7. Success! Market data is streaming
```

## Next Steps

### Run Full Trading System

Once market data is working, run the full system:

```bash
# Terminal 1: Chat node (LLM)
./run.sh python deploy_chat_node.py

# Terminal 2: Router node (trading agent)
./run.sh python deploy_router_node.py --strategy momentum

# Terminal 3: Market data
./run.sh python unified_market_connector.py \
    --provider topstepx \
    --symbols CON.F.US.ES.H26

# Terminal 4: Dashboard
./run.sh python tools_and_dashboard.py

# Terminal 5: Response viewer
./run.sh python response_viewer.py
```

### Example Strategies

See `deploy_router_node.py` for available strategies:

- `momentum` - Momentum-based trading
- `scalper` - Quick scalping strategy
- `brainrot` - Experimental strategy
- `default` - Basic trading strategy

## API Reference

For more details, see the official TopstepX API documentation:

- **Connection URLs:** https://gateway.docs.projectx.com/docs/getting-started/connection-urls
- **Authentication:** https://gateway.docs.projectx.com/docs/getting-started/authenticate/authenticate-api-key
- **Market Data:** https://gateway.docs.projectx.com/docs/category/market-data
- **Realtime Updates:** https://gateway.docs.projectx.com/docs/realtime/

## Support

- **Full Documentation:** See `ADAPTER_README.md`
- **Authentication Guide:** See `TOPSTEPX_AUTH_GUIDE.md`
- **Environment Setup:** See `ENV_SETUP.md`
- **Troubleshooting:** See `QUICK_FIX.md`

## Summary

**To get started:**

1. Create `.env` with your TopstepX credentials
2. Set `TOPSTEPX_ENVIRONMENT=topstepx-direct`
3. Set `TOPSTEPX_API_URL=https://api.topstepx.com`
4. Run: `./run.sh python unified_market_connector.py --provider topstepx --symbols CON.F.US.ES.H26`

**That's it!** Your TopstepX market data should now be streaming.
