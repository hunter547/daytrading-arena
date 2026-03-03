# TopstepX Integration with Existing Arena

This guide shows how to add TopstepX futures trading to your existing crypto trading arena using the **same deployment structure**.

## What Was Added

✅ TopstepX tools are now integrated into the **existing** `deploy_router_node.py`  
✅ New strategy: `--strategy futures` for TopstepX trading  
✅ Tools auto-detect if TopstepX is available

## Quick Start

Follow the **same workflow** as the original README, just with TopstepX additions:

### 1. Start Broker (Same as before)
```bash
cd calfkit-broker && make dev-up
```

### 2. Start Tools & Dashboard (Same as before)
```bash
./run.sh python tools_and_dashboard.py --bootstrap-servers localhost:9092
```

### 3. Deploy TopstepX Trading Tools Service (NEW)
In a new terminal:
```bash
./run.sh python topstepx_trading_tools.py --bootstrap-servers localhost:9092
```

This makes TopstepX tools available to agents.

### 4. Start Market Data Feed (NEW - Optional)
For TopstepX futures price data:
```bash
./run.sh python unified_market_connector.py \
  --provider topstepx \
  --symbols CON.F.US.MES.H26 CON.F.US.MNQ.H26 \
  --bootstrap-servers localhost:9092 \
  --interval 5
```

Or keep using the original Coinbase connector for crypto:
```bash
./run.sh python coinbase_connector.py --bootstrap-servers localhost:9092
```

### 5. Deploy ChatNode (Same as before)
```bash
./run.sh python deploy_chat_node.py \
  --name gpt4 \
  --model-id gpt-4o \
  --bootstrap-servers localhost:9092 \
  --api-key $OPENAI_API_KEY
```

### 6. Deploy Agent Router (UPDATED)

**For Crypto Trading (Original):**
```bash
./run.sh python deploy_router_node.py \
  --name momentum-trader \
  --chat-node-name gpt4 \
  --strategy momentum \
  --bootstrap-servers localhost:9092
```

**For Futures Trading (NEW):**
```bash
./run.sh python deploy_router_node.py \
  --name futures-trader \
  --chat-node-name gpt4 \
  --strategy futures \
  --bootstrap-servers localhost:9092
```

## Available Strategies

Original strategies (for crypto):
- `default` - Basic crypto day trading
- `momentum` - Momentum-based crypto trading
- `brainrot` - YOLO crypto trading
- `scalper` - High-frequency crypto scalping

**NEW strategy (for futures):**
- `futures` - TopstepX futures trading with risk management

## Tools Available to Agents

### Original Crypto Tools
- `execute_trade(product_id, quantity, action)` - Trade crypto on Coinbase
- `get_portfolio()` - View crypto portfolio
- `calculator(expression)` - Math calculations

### NEW TopstepX Tools (auto-added if service is running)
- `topstepx_buy(contract, quantity)` - Buy futures contracts
- `topstepx_sell(contract, quantity)` - Sell futures contracts  
- `topstepx_portfolio()` - View TopstepX portfolio

**Agents automatically get access to both sets of tools!**

## Example: Multi-Asset Trading Arena

Run agents trading **both crypto and futures** simultaneously:

**Terminal 1** - Broker (required):
```bash
cd calfkit-broker && make dev-up
```

**Terminal 2** - Dashboard:
```bash
./run.sh python tools_and_dashboard.py --bootstrap-servers localhost:9092
```

**Terminal 3** - Coinbase Market Data:
```bash
./run.sh python coinbase_connector.py --bootstrap-servers localhost:9092
```

**Terminal 4** - TopstepX Tools:
```bash
./run.sh python topstepx_trading_tools.py --bootstrap-servers localhost:9092
```

**Terminal 5** - TopstepX Market Data:
```bash
./run.sh python unified_market_connector.py \
  --provider topstepx --symbols CON.F.US.MES.H26 \
  --bootstrap-servers localhost:9092
```

**Terminal 6** - ChatNode:
```bash
./run.sh python deploy_chat_node.py \
  --name gpt4 --model-id gpt-4o \
  --bootstrap-servers localhost:9092 \
  --api-key $OPENAI_API_KEY
```

**Terminal 7** - Crypto Agent:
```bash
./run.sh python deploy_router_node.py \
  --name crypto-momentum \
  --chat-node-name gpt4 \
  --strategy momentum \
  --bootstrap-servers localhost:9092
```

**Terminal 8** - Futures Agent:
```bash
./run.sh python deploy_router_node.py \
  --name futures-trader \
  --chat-node-name gpt4 \
  --strategy futures \
  --bootstrap-servers localhost:9092
```

Now you have **2 agents trading simultaneously** - one on crypto, one on futures!

## How It Works

The integration follows the **exact same pattern** as the original:

1. **Tools are separate services** - TopstepX tools run independently
2. **Agents discover tools** - Routers automatically find available tools
3. **Strategies select behavior** - Use `--strategy futures` for TopstepX trading
4. **Dashboard shows everything** - All accounts (crypto + futures) in one view

## Files Modified

- ✅ `deploy_router_node.py` - Added TopstepX tools import and `futures` strategy
- ✅ `trading_tools.py` - Integrated TopstepX account display

## Files Added

- `topstepx_trading_tools.py` - TopstepX tools service (like crypto tools)
- `topstepx_account.py` - TopstepX account management
- `unified_market_connector.py` - Multi-provider market data

## No Breaking Changes

Everything works exactly as before! The original crypto trading arena is unchanged:
- Same deployment commands
- Same strategies work
- Dashboard shows crypto accounts as before

TopstepX is **additive** - it only activates if you:
1. Deploy the TopstepX tools service
2. Use the `futures` strategy

## Next Steps

Use the **original README workflow**, just add:
- Deploy `topstepx_trading_tools.py` as a service
- Use `--strategy futures` when deploying futures agents
- That's it!

Everything else stays the same. 🚀
