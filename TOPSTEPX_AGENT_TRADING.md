# TopstepX Agent Trading Setup

This guide shows how to connect AI trading agents to your TopstepX practice account for real futures trading.

## Prerequisites

1. **TopstepX JWT Token** - Set in `.env`:
   ```bash
   TOPSTEPX_JWT_TOKEN=your_token_here
   ```
   
   Get your token by running:
   ```bash
   ./run.sh python topstepx_auth.py
   ```

2. **Kafka Running** - Required for agent communication:
   ```bash
   # Start Kafka (if not already running)
   docker-compose up -d kafka
   ```

## Quick Start

### Step 1: Deploy TopstepX Trading Tools

Start the TopstepX trading tool service:

```bash
./run.sh python topstepx_trading_tools.py --bootstrap-servers localhost:9092
```

You should see:
```
✓ Trading enabled on practice account: 19424999

Tools are ready for agent requests!
Agents can now call:
  - topstepx_buy(contract, quantity)
  - topstepx_sell(contract, quantity)
  - topstepx_portfolio()
```

Keep this running in one terminal.

### Step 2: Deploy Market Data Connector

In another terminal, start the market data feed for futures contracts:

```bash
./run.sh python unified_market_connector.py \
  --provider topstepx \
  --symbols CON.F.US.MES.H26 CON.F.US.MNQ.H26 \
  --bootstrap-servers localhost:9092 \
  --interval 5
```

This streams live market data for:
- **MES** (Micro E-mini S&P 500)
- **MNQ** (Micro E-mini Nasdaq-100)

### Step 3: Start Your Trading Agent

Deploy your trading agent with access to TopstepX tools:

```bash
# Example: Deploy a futures trading agent
./run.sh python your_agent.py --bootstrap-servers localhost:9092
```

Your agent now has access to these tools:

#### `topstepx_buy(contract, quantity)`
Buy futures contracts (go LONG or close SHORT positions).

**Example:**
```python
# Agent buys 1 Micro E-mini S&P contract
result = await topstepx_buy(
    contract="CON.F.US.MES.H26",
    quantity=1
)
```

#### `topstepx_sell(contract, quantity)`
Sell futures contracts (go SHORT or close LONG positions).

**Example:**
```python
# Agent sells 2 Micro E-mini Nasdaq contracts
result = await topstepx_sell(
    contract="CON.F.US.MNQ.H26",
    quantity=2
)
```

#### `topstepx_portfolio()`
Get current portfolio status including positions and P&L.

**Example:**
```python
# Agent checks its portfolio
portfolio = await topstepx_portfolio()
# Returns:
# 📊 TopstepX Portfolio (Account: PRAC-V2-157469-77399797)
#   Equity: $50,000.00
#   Balance: $49,500.00
#   Positions:
#     CON.F.US.MES.H26: LONG 1 @ $5,800.00 (P&L: +$125.00)
```

## Available Contracts

Common futures contracts you can trade:

| Contract ID | Description | Tick Size | Contract Size |
|------------|-------------|-----------|---------------|
| `CON.F.US.MES.H26` | Micro E-mini S&P 500 | $1.25 | $5 × index |
| `CON.F.US.MNQ.H26` | Micro E-mini Nasdaq-100 | $0.50 | $2 × index |
| `CON.F.US.ES.H26` | E-mini S&P 500 | $12.50 | $50 × index |
| `CON.F.US.NQ.H26` | E-mini Nasdaq-100 | $5.00 | $20 × index |

**Note:** Contract codes follow the format:
- `CON.F.US.{SYMBOL}.{MONTH}{YEAR}`
- Month codes: H=March, M=June, U=September, Z=December
- Example: `H26` = March 2026

To find available contracts:
```bash
./run.sh python list_topstepx_contracts.py
```

## Monitoring

### View Account Dashboard

Monitor your TopstepX accounts in real-time:

```bash
./run.sh python topstepx_dashboard.py
```

Shows:
- Account balances
- Open positions
- Unrealized P&L
- Auto-refreshes every 5 seconds

### View Combined Dashboard

See both simulated and TopstepX accounts together:

```bash
./run.sh python tools_and_dashboard.py --bootstrap-servers localhost:9092
```

## Example Agent Workflow

Here's how an agent might use these tools:

```python
# 1. Check current portfolio
portfolio = await topstepx_portfolio()
# No positions, $50,000 balance

# 2. Receive market data signal
# Market Update: CON.F.US.MES.H26
# Last Price: $5,800.00
# Signal: Bullish momentum detected

# 3. Enter long position
result = await topstepx_buy(
    contract="CON.F.US.MES.H26",
    quantity=2
)
# ✓ BUY order placed successfully
# Order ID: 123456

# 4. Monitor position
portfolio = await topstepx_portfolio()
# Position: LONG 2 @ $5,800.00

# 5. Exit when target reached
result = await topstepx_sell(
    contract="CON.F.US.MES.H26",
    quantity=2
)
# ✓ SELL order placed successfully
```

## Safety Features

- **Practice Account Only**: Tools automatically use the practice account (PRAC-V2-*)
- **Market Orders**: All orders are market orders for immediate execution
- **Size Limits**: Enforced by TopstepX account rules
- **Real-time Positions**: Agents can always check their current positions before trading

## Troubleshooting

### "No practice account found"
Make sure your TopstepX account includes a practice account. Check available accounts:
```bash
./run.sh python topstepx_account.py
```

### "Order failed: Insufficient buying power"
The practice account has limited margin. Check your current positions:
```bash
./run.sh python topstepx_account.py --account-id 19424999
```

### "Contract not found"
Verify the contract ID is correct and currently available:
```bash
./run.sh python list_topstepx_contracts.py --search MES
```

### Token expired
JWT tokens expire after 24 hours. Refresh your token:
```bash
./run.sh python topstepx_auth.py
# Copy new token to .env
```

## Architecture

```
┌─────────────────────┐
│  Trading Agent      │
│  (AI/LLM)          │
└──────┬──────────────┘
       │ Kafka messages
       ↓
┌─────────────────────┐         ┌──────────────────┐
│ TopstepX Trading    │────────→│  TopstepX API    │
│ Tools Service       │  HTTPS  │  (Live Trading)  │
└─────────────────────┘         └──────────────────┘
       ↑
       │ Market data (Kafka)
       │
┌─────────────────────┐         ┌──────────────────┐
│ Market Data         │────────→│  TopstepX        │
│ Connector           │ WebSocket│  Market Data    │
└─────────────────────┘         └──────────────────┘
```

## Next Steps

1. **Test with simple agent**: Start with an agent that just checks the portfolio
2. **Add market data**: Connect the unified market connector for price feeds
3. **Implement strategy**: Have your agent analyze market data and make trades
4. **Monitor results**: Use the dashboards to watch your agent trade in real-time

## Support

- TopstepX API Docs: https://api.topstepx.com/swagger
- Issues: File in your project's issue tracker
