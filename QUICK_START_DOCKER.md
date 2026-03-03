# Quick Start: Docker Compose Edition

This is the **easiest way** to run the entire futures trading system. Instead of managing 7 terminal windows, use Docker Compose to run everything with a single command.

## Prerequisites

1. **Docker Desktop** installed and running
2. **Your API keys** (TopstepX + OpenAI)

## 3-Step Quick Start

### Step 1: Configure Environment

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env and add your credentials
nano .env  # or use your favorite editor
```

Required in `.env`:
```bash
TOPSTEPX_USERNAME=your-email@example.com
TOPSTEPX_API_KEY=your-api-key-here
TOPSTEPX_JWT_TOKEN=your-jwt-token-here
TOPSTEPX_ENVIRONMENT=demo
OPENAI_API_KEY=sk-your-openai-key-here
```

### Step 2: Start Everything

**Option A: Using the start script (recommended)**
```bash
./docker-start.sh
```

**Option B: Using Make**
```bash
make build
make up
```

**Option C: Using docker-compose directly**
```bash
docker-compose build
docker-compose up -d
```

### Step 3: Monitor Your Agent

**Important:** This system uses a terminal-based dashboard, not a web UI. There is no http://localhost:8501 interface.

View agent activity via logs:
```bash
docker-compose logs -f agent
```

View market data:
```bash
docker-compose logs -f market-connector
```

## What's Running?

The system starts **8 containers**:

| Container | Purpose |
|-----------|---------|
| `futures-zookeeper` | Kafka coordination |
| `futures-kafka` | Message broker |
| `futures-market-connector` | TopstepX market data stream |
| `futures-trading-tools` | Trade execution tools |
| `futures-dashboard` | Trading tools (terminal UI, view via logs) |
| `futures-chatnode` | GPT-5 Nano AI |
| `futures-agent` | Your trading agent |
| `futures-response-viewer` | (Optional) Agent reasoning viewer |

## Common Commands

### Monitoring
```bash
# View all logs
docker-compose logs -f

# View specific service
docker-compose logs -f agent
docker-compose logs -f market-connector
docker-compose logs -f chatnode

# Check service status
docker-compose ps

# Use Makefile shortcuts
make logs
make logs-agent
make ps
```

### Control
```bash
# Stop everything
docker-compose down

# Restart a service
docker-compose restart agent

# Rebuild after code changes
docker-compose up -d --build

# Or use Make
make restart
make rebuild
```

### Enable Response Viewer
```bash
# Start with response viewer
docker-compose --profile monitoring up -d

# Or use Make
make up-monitor

# View its logs
docker-compose logs -f response-viewer
```

## Troubleshooting

### Services won't start
```bash
# Check Docker
docker ps

# View logs
docker-compose logs

# Try clean restart
docker-compose down -v
docker-compose up -d
```

### No agent activity
```bash
# 1. Check all services are running
docker-compose ps

# 2. View agent logs
docker-compose logs -f agent

# 3. Check market data is flowing
docker-compose logs -f market-connector

# 4. Verify ChatNode is running
docker-compose ps chatnode
```

### Dashboard won't load
```bash
# Check dashboard status
docker-compose ps dashboard

# View logs
docker-compose logs -f dashboard

# Restart it
docker-compose restart dashboard
```

## Stop Everything

```bash
# Stop all services
docker-compose down

# Stop and remove everything (clean slate)
docker-compose down -v

# Or use Make
make down
make clean
```

## vs. Manual 7-Terminal Setup

### Docker Compose ✅
- ✅ One command to start everything
- ✅ Automatic dependency management
- ✅ Built-in health checks
- ✅ Auto-restart on failures
- ✅ Centralized logging
- ✅ Easy to share/reproduce

### Manual Terminals ✅
- ✅ See individual logs more easily
- ✅ More control during development
- ✅ No Docker required

## Next Steps

1. **Monitor the dashboard** at http://localhost:8501
2. **Watch agent reasoning** with response viewer
3. **Adjust strategies** in `deploy_router_node.py` and rebuild
4. **Add more agents** by editing `docker-compose.yml`

## Files Created

- `Dockerfile` - Base image for all services
- `docker-compose.yml` - Service orchestration
- `docker-start.sh` - Quick start script
- `Makefile` - Convenient shortcuts
- `.dockerignore` - Exclude unnecessary files
- `DOCKER_SETUP.md` - Detailed documentation

## Help

```bash
# See all Make commands
make help

# Check system health
make health

# View Kafka topics
make kafka-topics
```

For more details, see **DOCKER_SETUP.md**
