# Important Note About the Dashboard

## No Web UI Available

This trading system **does not have a web-based dashboard**. The "dashboard" service runs a **terminal-based UI** using the Rich library, which is designed to run in an interactive terminal (TTY).

### What This Means

- ❌ There is **NO** http://localhost:8501 web interface
- ❌ You cannot view the dashboard in a browser
- ✅ The dashboard runs in the terminal where you start the services
- ✅ In Docker, you can view dashboard output via logs

### How to View Trading Activity

#### Option 1: View Logs (Docker)

```bash
# View all dashboard output
docker-compose logs -f dashboard

# View agent reasoning and decisions
docker-compose logs -f response-viewer

# View agent activity
docker-compose logs -f agent

# View market data
docker-compose logs -f market-connector
```

#### Option 2: Run Services Manually (with Terminal UI)

If you want to see the interactive Rich terminal dashboard, you need to run the services **outside Docker** in separate terminals:

```bash
# Terminal 1: Kafka (keep Docker for this)
cd ../calfkit-broker && make dev-up

# Terminal 2: Tools & Dashboard (you'll see the Rich UI here!)
./run.sh python tools_and_dashboard.py --bootstrap-servers localhost:9092

# Terminal 3: TopstepX Trading Tools
./run.sh python topstepx_trading_tools.py --bootstrap-servers localhost:9092

# Terminal 4: Market Data
./run.sh python unified_market_connector.py \
  --provider topstepx \
  --symbols CON.F.US.MES.H26 CON.F.US.MNQ.H26 \
  --bootstrap-servers localhost:9092 \
  --interval 5

# Terminal 5: ChatNode
./run.sh python deploy_chat_node.py \
  --name gpt5-nano \
  --model-id gpt-5-nano \
  --bootstrap-servers localhost:9092

# Terminal 6: Agent
./run.sh python deploy_router_node.py \
  --name futures-trader \
  --chat-node-name gpt5-nano \
  --strategy futures \
  --bootstrap-servers localhost:9092

# Terminal 7: Response Viewer
./run.sh python response_viewer.py --bootstrap-servers localhost:9092
```

When running manually, **Terminal 2** will show the interactive Rich dashboard with live portfolio updates.

### What Can You See in Docker?

Even without the interactive terminal UI, you can still monitor everything via logs:

1. **Trading Activity** - Watch agent make decisions
   ```bash
   docker-compose logs -f agent
   ```

2. **Agent Reasoning** - See why agent makes trades
   ```bash
   docker-compose logs -f response-viewer
   ```

3. **Market Data** - View price updates
   ```bash
   docker-compose logs -f market-connector
   ```

4. **Tool Calls** - See trades being executed
   ```bash
   docker-compose logs -f dashboard
   docker-compose logs -f trading-tools
   ```

### Why Use Docker Then?

Docker is still valuable because it:
- ✅ Manages all service dependencies automatically
- ✅ Ensures correct startup order
- ✅ Provides centralized logging
- ✅ Makes it easy to start/stop everything
- ✅ Captures all agent activity in searchable logs

You just won't get the fancy terminal UI - but all the **functionality is the same**.

### Recommended Approach

**For Active Trading/Monitoring:**
- Use manual 7-terminal setup (see `START_FUTURES_TRADING.md`)
- You'll see the Rich terminal dashboard in Terminal 2

**For Background Running/Testing:**
- Use Docker Compose (see `QUICK_START_DOCKER.md`)
- Monitor via logs instead of dashboard UI

### Creating a Web Dashboard (Future Enhancement)

If you want to build a web-based dashboard, you could:

1. Create a Streamlit app that reads from Kafka topics
2. Add a FastAPI service that exposes portfolio data as REST API
3. Build a React/Next.js frontend that polls the API

But that's not currently part of this system.
