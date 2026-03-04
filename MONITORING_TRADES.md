# How to Monitor Trades in Docker Logs 📊

Quick reference for detecting when the AI agent places trades on your TopstepX practice account.

---

## 🎯 Quick Commands

### **Watch for ANY trade activity**
```bash
docker compose logs -f trading-tools | grep -E "BUY|SELL|EXECUTING|ORDER"
```

### **Watch for portfolio checks**
```bash
docker compose logs -f trading-tools | grep -E "PORTFOLIO|POSITION"
```

### **Watch ALL agent activity**
```bash
docker compose logs -f agent chatnode trading-tools
```

---

## 🔍 What to Look For

### **1. Portfolio Check** 📊
The agent checks its portfolio before making decisions:

```
futures-trading-tools  | 📊 CHECKING PORTFOLIO STATUS for account 19424999
futures-trading-tools  | 💼 PORTFOLIO: No open positions | Equity: $50,000.00
```

or if there are positions:

```
futures-trading-tools  | 💼 POSITION: CON.F.US.MES.H26: LONG 2 @ $6,823.50 (P&L: +$125.00)
```

---

### **2. Buy Order Placed** 🔵
When the agent decides to buy:

```
futures-trading-tools  | 🔵 EXECUTING BUY ORDER: 1x CON.F.US.MES.H26
futures-trading-tools  | ✅ BUY ORDER SUCCESSFUL: 1x CON.F.US.MES.H26 | Order ID: 12345678
```

---

### **3. Sell Order Placed** 🔴
When the agent decides to sell:

```
futures-trading-tools  | 🔴 EXECUTING SELL ORDER: 1x CON.F.US.MES.H26
futures-trading-tools  | ✅ SELL ORDER SUCCESSFUL: 1x CON.F.US.MES.H26 | Order ID: 12345679
```

---

### **4. Order Failed** ❌
If something goes wrong:

```
futures-trading-tools  | ❌ BUY ORDER FAILED: 1x CON.F.US.MES.H26 | Error: Insufficient margin
```

---

## 📈 Complete Trade Flow Example

Here's what a complete trade cycle looks like in the logs:

```bash
# 1. Market data arrives (every 60 seconds)
futures-market-connector  | Fetched 19 bars for CON.F.US.MES.H26

# 2. Agent receives data and analyzes
futures-agent  | agent_router.input | Received
futures-agent  | agent_router.input | Processed

# 3. Agent asks ChatNode (GPT-5 Nano) for decision
futures-chatnode  | ai_prompted.gpt5-nano | Received
futures-chatnode  | ai_prompted.gpt5-nano | Processed

# 4. Agent checks portfolio first
futures-trading-tools  | 📊 CHECKING PORTFOLIO STATUS for account 19424999
futures-trading-tools  | 💼 PORTFOLIO: No open positions | Equity: $50,000.00

# 5. Agent decides to buy
futures-trading-tools  | 🔵 EXECUTING BUY ORDER: 1x CON.F.US.MES.H26
futures-trading-tools  | ✅ BUY ORDER SUCCESSFUL: 1x CON.F.US.MES.H26 | Order ID: 12345678

# 6. Next cycle - agent checks portfolio again
futures-trading-tools  | 📊 CHECKING PORTFOLIO STATUS for account 19424999
futures-trading-tools  | 💼 POSITION: CON.F.US.MES.H26: LONG 1 @ $6,823.50 (P&L: +$25.00)

# 7. Agent decides to close position
futures-trading-tools  | 🔴 EXECUTING SELL ORDER: 1x CON.F.US.MES.H26
futures-trading-tools  | ✅ SELL ORDER SUCCESSFUL: 1x CON.F.US.MES.H26 | Order ID: 12345679
```

---

## 🛠️ Useful Monitoring Commands

### **Tail logs in real-time**
```bash
# All services
docker compose logs -f

# Just trading activity
docker compose logs -f trading-tools

# Agent decisions
docker compose logs -f agent

# Market data updates
docker compose logs -f market-connector
```

### **Search historical logs**
```bash
# Find all executed trades
docker compose logs trading-tools | grep "ORDER SUCCESSFUL"

# Count total trades
docker compose logs trading-tools | grep -c "ORDER SUCCESSFUL"

# Find specific contract trades
docker compose logs trading-tools | grep "CON.F.US.MES.H26"

# Show last 100 lines
docker compose logs --tail=100 trading-tools
```

### **Check specific time range**
```bash
# Logs from last 10 minutes
docker compose logs --since=10m trading-tools

# Logs from specific time
docker compose logs --since="2026-03-03T20:00:00" trading-tools
```

---

## 🎨 Log Symbols Reference

| Symbol | Meaning |
|--------|---------|
| 🔵 | Buy order being placed |
| 🔴 | Sell order being placed |
| ✅ | Order executed successfully |
| ❌ | Order failed |
| 📊 | Portfolio check |
| 💼 | Position information |

---

## 📊 Why No Trades Yet?

If you don't see any trades, it means:

1. **Agent is being conservative** ✅ (GOOD!)
   - Waiting for clear signals
   - Following risk management rules
   - Not trading randomly

2. **Agent is analyzing** 📈
   - Checking portfolio every cycle
   - Evaluating market conditions
   - Looking for high-confidence opportunities

3. **Market conditions** 🌊
   - No strong trends detected
   - Volatility too low/high
   - Consolidation phase

---

## 🔔 Set Up Alerts (Optional)

### **Get notified on trades**
```bash
# Watch for trades and make a sound
docker compose logs -f trading-tools | grep --line-buffered "ORDER SUCCESSFUL" | while read line; do echo -e "\a$line"; done
```

### **Save trades to file**
```bash
# Log all trades to trades.log
docker compose logs -f trading-tools | grep "ORDER SUCCESSFUL" >> trades.log
```

---

## 🚀 Quick Test

To verify everything is working, watch all activity:

```bash
docker compose logs -f | grep -E "EXECUTING|SUCCESSFUL|FAILED|PORTFOLIO|POSITION"
```

You should see **portfolio checks every ~60 seconds** even if no trades are placed.

---

## 📱 Mobile Monitoring (Advanced)

If you want to monitor from your phone:

1. **Set up webhook notifications** (using services like Zapier/IFTTT)
2. **Use Docker logs API** with a monitoring dashboard
3. **Configure email alerts** when trades execute

---

## ✅ Healthy System Indicators

You should see these every 60 seconds:

```
✅ Candle refresh loop running
✅ Agent receiving market updates (2 per minute)
✅ ChatNode processing requests
✅ Portfolio checks happening
```

If you see all of these, **the system is working correctly** and waiting for the right opportunity to trade!

---

**Remember:** Conservative trading = Better long-term results 📈
