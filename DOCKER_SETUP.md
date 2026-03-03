# Docker Compose Setup for Futures Trading System

This guide explains how to run the entire 7-service futures trading system using Docker Compose instead of managing 7 separate terminal windows.

## Architecture

The system consists of 7 interconnected services:

1. **Zookeeper** - Coordination service for Kafka
2. **Kafka Broker** - Message broker for all inter-service communication
3. **Market Data Connector** - Streams TopstepX market data to Kafka (THIS TRIGGERS THE AGENT!)
4. **TopstepX Trading Tools** - Provides trading execution tools (buy/sell/portfolio)
5. **Tools & Dashboard** - Web UI at http://localhost:8501
6. **ChatNode (GPT-5 Nano)** - AI inference engine
7. **Agent Router** - The AI trading agent that makes decisions
8. **Response Viewer** - (Optional) Shows live agent reasoning and tool calls

## Prerequisites

1. **Docker Desktop** installed and running
2. **Environment variables** configured in `.env` file
3. **API Keys** for TopstepX and OpenAI

## Quick Start

### 1. Configure Environment Variables

Copy the example environment file and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env` and add your credentials:

```bash
# Required - TopstepX credentials
TOPSTEPX_USERNAME=your-email@example.com
TOPSTEPX_API_KEY=your-api-key-here
TOPSTEPX_JWT_TOKEN=your-jwt-token-here
TOPSTEPX_ENVIRONMENT=demo

# Required - OpenAI API key
OPENAI_API_KEY=sk-your-openai-key-here

# Optional - Customize futures symbols (default: MES and MNQ)
TOPSTEPX_SYMBOLS=CON.F.US.MES.H26,CON.F.US.MNQ.H26
```

### 2. Build and Start All Services

```bash
# Build Docker images (first time only)
docker-compose build

# Start all services (without response viewer)
docker-compose up -d

# Or, start all services including response viewer
docker-compose --profile monitoring up -d
```

### 3. Monitor the System

```bash
# View logs from all services
docker-compose logs -f

# View logs from a specific service
docker-compose logs -f agent
docker-compose logs -f market-connector
docker-compose logs -f chatnode

# Check service status
docker-compose ps
```

### 4. Access the Dashboard

Open your browser to:
- **Trading Dashboard**: http://localhost:8501

### 5. Stop All Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

## Service Details

### Service Names and Containers

| Service | Container Name | Purpose | Port |
|---------|----------------|---------|------|
| `zookeeper` | futures-zookeeper | Kafka coordination | 2181 |
| `kafka` | futures-kafka | Message broker | 9092, 29092 |
| `market-connector` | futures-market-connector | TopstepX market data | - |
| `trading-tools` | futures-trading-tools | TopstepX trade execution | - |
| `dashboard` | futures-dashboard | Web UI | 8501 |
| `chatnode` | futures-chatnode | GPT-5 Nano inference | - |
| `agent` | futures-agent | AI trading agent | - |
| `response-viewer` | futures-response-viewer | Agent activity monitor | - |

### Service Dependencies

```
zookeeper
  ↓
kafka (requires zookeeper to be healthy)
  ↓
market-connector, trading-tools, dashboard, chatnode (require kafka)
  ↓
agent (requires kafka + chatnode + market-connector + trading-tools)
```

## Common Commands

### Start/Stop Individual Services

```bash
# Start a specific service
docker-compose up -d market-connector

# Stop a specific service
docker-compose stop agent

# Restart a service
docker-compose restart chatnode
```

### View Logs

```bash
# All logs
docker-compose logs -f

# Specific service logs
docker-compose logs -f agent
docker-compose logs -f market-connector
docker-compose logs -f chatnode

# Last 100 lines
docker-compose logs --tail=100 agent
```

### Debugging

```bash
# Enter a container shell
docker-compose exec agent bash

# Check Kafka topics
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Check environment variables
docker-compose exec agent env | grep TOPSTEPX
docker-compose exec chatnode env | grep OPENAI
```

### Rebuild After Code Changes

```bash
# Rebuild all services
docker-compose build

# Rebuild specific service
docker-compose build agent

# Rebuild and restart
docker-compose up -d --build
```

## Configuration Options

### Change the AI Model

Edit `docker-compose.yml` and modify the `chatnode` service:

```yaml
chatnode:
  command: >
    python deploy_chat_node.py
    --name gpt4o-mini
    --model-id gpt-4o-mini
    --bootstrap-servers kafka:9092
```

Don't forget to update the agent's `--chat-node-name` to match:

```yaml
agent:
  command: >
    python deploy_router_node.py
    --name futures-trader
    --chat-node-name gpt4o-mini
    --strategy futures
    --bootstrap-servers kafka:9092
```

### Change Market Data Update Frequency

Edit the `market-connector` service:

```yaml
market-connector:
  command: >
    python unified_market_connector.py
    --provider topstepx
    --symbols CON.F.US.MES.H26 CON.F.US.MNQ.H26
    --bootstrap-servers kafka:9092
    --interval 10  # Changed from 5 to 10 seconds
```

### Add More Futures Contracts

Update your `.env` file:

```bash
TOPSTEPX_SYMBOLS=CON.F.US.MES.H26,CON.F.US.MNQ.H26,CON.F.US.MYM.H26
```

Or edit `docker-compose.yml`:

```yaml
market-connector:
  command: >
    python unified_market_connector.py
    --provider topstepx
    --symbols CON.F.US.MES.H26 CON.F.US.MNQ.H26 CON.F.US.MYM.H26
    --bootstrap-servers kafka:9092
    --interval 5
```

### Enable Response Viewer

The response viewer is optional and runs under the `monitoring` profile:

```bash
# Start with response viewer
docker-compose --profile monitoring up -d

# View response viewer logs
docker-compose logs -f response-viewer
```

## Troubleshooting

### Services Won't Start

```bash
# Check Docker is running
docker ps

# Check service status
docker-compose ps

# View all logs
docker-compose logs

# Restart all services
docker-compose restart
```

### No Market Data Arriving

```bash
# Check market connector logs
docker-compose logs -f market-connector

# Verify TopstepX credentials
docker-compose exec market-connector env | grep TOPSTEPX

# Check Kafka topics
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Agent Not Trading

```bash
# 1. Check if ChatNode is running
docker-compose ps chatnode

# 2. Check agent logs
docker-compose logs -f agent

# 3. Check if market data is flowing
docker-compose logs -f market-connector

# 4. View agent reasoning (start response viewer)
docker-compose --profile monitoring up -d response-viewer
docker-compose logs -f response-viewer
```

### Dashboard Not Loading

```bash
# Check dashboard service
docker-compose ps dashboard

# View dashboard logs
docker-compose logs -f dashboard

# Restart dashboard
docker-compose restart dashboard

# Access at http://localhost:8501
```

### OpenAI API Errors

```bash
# Check API key is set
docker-compose exec chatnode env | grep OPENAI_API_KEY

# View chatnode logs
docker-compose logs -f chatnode
```

### Kafka Connection Issues

```bash
# Check Kafka health
docker-compose ps kafka

# View Kafka logs
docker-compose logs -f kafka

# Restart Kafka
docker-compose restart kafka
```

## Differences from Manual Terminal Setup

### Manual (7 Terminals)
- ✅ More control over individual services
- ✅ Easier to see individual logs
- ❌ Requires managing 7 terminal windows
- ❌ Manual startup order coordination
- ❌ Services keep running after terminal closes

### Docker Compose
- ✅ Single command to start/stop everything
- ✅ Automatic service dependency management
- ✅ Built-in health checks
- ✅ Automatic restarts on failure
- ✅ Centralized logging
- ❌ Slightly more complex initial setup
- ❌ Requires rebuilding after code changes

## Networking Notes

Inside Docker Compose, services use internal hostnames:
- Kafka broker: `kafka:9092` (internal) or `localhost:29092` (from host)
- Dashboard: `http://localhost:8501` (accessible from host)

From your local machine (outside Docker):
- Kafka: `localhost:29092`
- Dashboard: `http://localhost:8501`

## Performance Notes

### Resource Usage

Typical resource usage:
- **Memory**: ~2-4 GB total
- **CPU**: Low when idle, spikes during agent reasoning
- **Network**: Minimal (local only)

### Scaling

To run multiple agents:

```bash
# Scale the agent service
docker-compose up -d --scale agent=3
```

Or manually add more agent services in `docker-compose.yml`:

```yaml
agent-momentum:
  build: .
  container_name: futures-agent-momentum
  command: >
    python deploy_router_node.py
    --name momentum-trader
    --chat-node-name gpt5-nano
    --strategy momentum
    --bootstrap-servers kafka:9092

agent-contrarian:
  build: .
  container_name: futures-agent-contrarian
  command: >
    python deploy_router_node.py
    --name contrarian-trader
    --chat-node-name gpt5-nano
    --strategy contrarian
    --bootstrap-servers kafka:9092
```

## Next Steps

1. **Monitor agent behavior** via dashboard at http://localhost:8501
2. **View agent reasoning** with `docker-compose --profile monitoring up -d`
3. **Adjust strategies** by editing `deploy_router_node.py` and rebuilding
4. **Add more agents** by scaling or adding services
5. **Track performance** and optimize prompts

## Support

- Check logs: `docker-compose logs -f`
- Review docs: `README.md`, `TOPSTEPX_QUICKSTART.md`
- File issues: GitHub repository

## Clean Up

```bash
# Stop all services
docker-compose down

# Remove all containers, networks, and volumes
docker-compose down -v

# Remove Docker images (reclaim disk space)
docker-compose down --rmi all -v
```
