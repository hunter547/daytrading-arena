# 🚀 Quick Start: AI Trading on TopstepX

Get your AI trading agent running in **3 simple steps**.

---

## Option 1: Automated Setup (Easiest)

Run the setup script:
```bash
./start_ai_trading.sh
```

This will:
- ✅ Check all prerequisites
- ✅ Start required services  
- ✅ Show you what to do next

---

## Option 2: Manual Setup (4 Terminals)

### Prerequisites
Make sure you have:
- `TOPSTEPX_JWT_TOKEN` in `.env`
- `OPENAI_API_KEY` in `.env`
- Kafka running

### Terminal 1: Trading Tools ⚡ (REQUIRED)
```bash
./run.sh python topstepx_trading_tools.py --bootstrap-servers localhost:9092
```
Keep this running! This connects your AI to TopstepX.

### Terminal 2: Market Data 📊 (Recommended)
```bash
./run.sh python unified_market_connector.py \
  --provider topstepx \
  --symbols CON.F.US.MES.H26 CON.F.US.MNQ.H26 \
  --bootstrap-servers localhost:9092 \
  --interval 5
```
Streams live price updates to your AI.

### Terminal 3: AI Agent 🤖 (The Trader)
```bash
# Start in ADVISOR mode first (safe - no trading)
./run.sh python ai_trader.py --bootstrap-servers localhost:9092 --advisor-mode

# Or start in AUTONOMOUS mode (will trade!)
./run.sh python ai_trader.py --bootstrap-servers localhost:9092
```

### Terminal 4: Dashboard 📈 (Optional)
```bash
./run.sh python tools_and_dashboard.py --bootstrap-servers localhost:9092
```
Watch your AI trade in real-time!

---

## What Happens Next?

1. **Market data arrives** → Price updates stream every 5 seconds
2. **AI analyzes** → GPT-4 evaluates the market
3. **AI decides** → Buy, sell, or hold
4. **Trade executes** → Order placed on TopstepX practice account
5. **You monitor** → Dashboard shows everything live

---

## Your Practice Account

- **Account ID**: 19424999
- **Type**: Practice Account
- **Balance**: $150,000
- **Eligible**: ✅ Yes (always eligible)

---

That's it! You're ready to trade with AI! 🎉
