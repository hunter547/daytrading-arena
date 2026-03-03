# Environment Configuration Guide

This guide explains how to set up your `.env` file for the crypto day trading arena.

## Quick Setup (Recommended)

### Option 1: Interactive Setup Script

Run the interactive setup script that will guide you through configuration:

```bash
./setup_env.sh
```

This will:
- Ask for your configuration values
- Create a `.env` file with your settings
- Set proper file permissions (600 for security)
- Show you next steps

### Option 2: Manual Setup

Copy the template and edit it:

```bash
cp .env.template .env
nano .env  # or use your preferred editor
```

Fill in your credentials and save.

## Configuration Options

### Required for All Users

```bash
# Kafka broker (for full trading system)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# OpenAI API key (for AI trading agents)
OPENAI_API_KEY=sk-your-openai-key-here
```

### For Coinbase Trading (Crypto)

No additional configuration needed! Coinbase works without authentication for market data.

### For TopstepX Trading (CME Futures)

**You need these from your TopstepX dashboard:**

```bash
# Your TopstepX credentials
TOPSTEPX_USERNAME=your-email@example.com
TOPSTEPX_API_KEY=your-api-key-from-topstepx

# Environment (demo for paper trading, topstepx for live)
TOPSTEPX_ENVIRONMENT=demo
```

**Optional (if you already have a JWT token):**

```bash
# JWT token (expires after 24 hours)
# Only needed if you're not using username + API key
TOPSTEPX_JWT_TOKEN=eyJhbGciOi...
```

## Complete .env Example

```bash
# ============================================================================
# Crypto Day Trading Arena - Environment Configuration
# ============================================================================

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# LLM Configuration
OPENAI_API_KEY=sk-proj-abc123xyz789...

# TopstepX Configuration (optional, for CME futures)
TOPSTEPX_USERNAME=john.doe@example.com
TOPSTEPX_API_KEY=abc123xyz789
TOPSTEPX_ENVIRONMENT=demo
```

## Loading Environment Variables

### For Linux/macOS

**Method 1: Export all variables**
```bash
set -a
source .env
set +a
```

**Method 2: Load in current shell** (doesn't export to subprocesses)
```bash
source .env
```

**Method 3: Use with command**
```bash
env $(cat .env | xargs) python unified_market_connector.py --provider coinbase --symbols BTC-USD
```

### For Windows (PowerShell)

```powershell
Get-Content .env | ForEach-Object {
    if ($_ -match '^([^=]+)=(.*)$') {
        [System.Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process')
    }
}
```

### For Windows (Command Prompt)

```cmd
for /f "tokens=*" %i in (.env) do set %i
```

## Verification

### Check if variables are loaded:

```bash
echo $KAFKA_BOOTSTRAP_SERVERS
echo $TOPSTEPX_USERNAME
echo $TOPSTEPX_API_KEY
```

### Test TopstepX authentication:

```bash
source venv/bin/activate
source .env
python topstepx_auth.py
```

Expected output if successful:
```
[INFO] Authenticating with TopstepX (demo)...
[INFO] ✓ Authentication successful! JWT token obtained.

======================================================================
SUCCESS! Your JWT Token:
======================================================================
eyJhbGciOi...
======================================================================
```

## Environment Variables Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Yes* | `localhost:9092` | Kafka broker addresses |
| `OPENAI_API_KEY` | Yes** | - | OpenAI API key for AI agents |
| `TOPSTEPX_USERNAME` | No*** | - | TopstepX username/email |
| `TOPSTEPX_API_KEY` | No*** | - | TopstepX API key |
| `TOPSTEPX_ENVIRONMENT` | No | `demo` | TopstepX environment name |
| `TOPSTEPX_JWT_TOKEN` | No | - | Pre-generated JWT token |

\* Required for full trading system with agents  
\** Required for AI trading agents  
\*** Required only if using TopstepX for CME futures

## TopstepX Environments

Select the environment that matches your platform:

| Platform | Environment Value |
|----------|------------------|
| Demo/Paper Trading | `demo` |
| TopstepX Live | `topstepx` |
| Alpha Ticks | `alpha-ticks` |
| Aqua Futures | `aqua-futures` |
| Blue Guardian | `blue-guardian` |
| Blusky | `blusky` |
| Day Traders | `day-traders` |
| E8X | `e8x` |
| Funding Futures | `funding-futures` |
| Holaprime | `holaprime` |
| Lucid Trading | `lucid-trading` |
| Phidias | `phidias` |
| TickTick Trader | `ticktick-trader` |
| TopOne Futures | `topone-futures` |
| Tradeify | `tradeify` |
| TX3 Funding | `tx3-funding` |

## Security Best Practices

### 1. Never Commit .env to Git

The `.env` file is already in `.gitignore`, but double-check:

```bash
git check-ignore .env
# Should output: .env
```

### 2. Set Proper File Permissions

```bash
chmod 600 .env  # Owner read/write only
```

### 3. Use Different Keys for Different Environments

```bash
# Development
OPENAI_API_KEY=sk-dev-key...
TOPSTEPX_ENVIRONMENT=demo

# Production
OPENAI_API_KEY=sk-prod-key...
TOPSTEPX_ENVIRONMENT=topstepx
```

### 4. Rotate API Keys Regularly

- Change keys every 90 days
- Use different keys for each project
- Revoke old keys after rotation

### 5. Use Secrets Management in Production

For production deployments, use:
- AWS Secrets Manager
- HashiCorp Vault
- Azure Key Vault
- Google Secret Manager

## Troubleshooting

### Issue: "Environment variable not found"

**Cause:** Variables not loaded or shell not restarted

**Solution:**
```bash
# Re-source the .env file
set -a; source .env; set +a

# Verify it's loaded
echo $TOPSTEPX_USERNAME
```

### Issue: "Authentication failed"

**Cause:** Incorrect credentials in .env

**Solution:**
```bash
# Check what's in your .env
cat .env | grep TOPSTEPX

# Verify values
echo "Username: $TOPSTEPX_USERNAME"
echo "API Key: $TOPSTEPX_API_KEY"
echo "Environment: $TOPSTEPX_ENVIRONMENT"
```

### Issue: "Permission denied: .env"

**Cause:** File permissions too restrictive

**Solution:**
```bash
chmod 600 .env  # Make readable/writable by owner
```

### Issue: ".env not found"

**Cause:** File doesn't exist

**Solution:**
```bash
# Create from template
cp .env.template .env

# Or run setup script
./setup_env.sh
```

## Usage Examples

### Example 1: Test Coinbase (No .env Required)

```bash
# Just run - no environment variables needed!
python unified_market_connector.py --provider coinbase --symbols BTC-USD
```

### Example 2: Test TopstepX with .env

```bash
# Load environment
source .env

# Test authentication
python topstepx_auth.py

# Run market connector
python unified_market_connector.py \
    --provider topstepx \
    --symbols CON.F.US.ES.H26
```

### Example 3: Run Full Trading System

```bash
# Load environment
set -a; source .env; set +a

# Activate virtual environment
source venv/bin/activate

# Start all components (in separate terminals)

# Terminal 1: Chat node
python deploy_chat_node.py

# Terminal 2: Router node
python deploy_router_node.py --strategy momentum

# Terminal 3: Market data
python unified_market_connector.py --provider coinbase --symbols BTC-USD ETH-USD

# Terminal 4: Dashboard
python tools_and_dashboard.py

# Terminal 5: Response viewer
python response_viewer.py
```

### Example 4: Run with Docker Compose

Create `docker-compose.yml`:

```yaml
version: '3.8'
services:
  trading-system:
    build: .
    env_file:
      - .env
    command: python unified_market_connector.py --provider coinbase --symbols BTC-USD
```

Then:
```bash
docker-compose up
```

## Alternative: Using direnv (Advanced)

Install [direnv](https://direnv.net/) for automatic environment loading:

```bash
# Install direnv
# macOS
brew install direnv

# Ubuntu/Debian
sudo apt-get install direnv

# Add to shell (bash)
echo 'eval "$(direnv hook bash)"' >> ~/.bashrc
source ~/.bashrc

# Allow direnv for this directory
echo "dotenv" > .envrc
direnv allow

# Now .env is automatically loaded when you cd into this directory!
cd /path/to/crypto-daytrading-arena
# Environment variables are now loaded automatically
```

## Summary

**Quickest way to get started:**

1. **Run setup script:**
   ```bash
   ./setup_env.sh
   ```

2. **Load environment:**
   ```bash
   source .env
   ```

3. **Test it:**
   ```bash
   python unified_market_connector.py --provider coinbase --symbols BTC-USD
   ```

**For TopstepX:**

1. **Get your credentials** from TopstepX dashboard
2. **Run setup script** and enter credentials
3. **Test authentication:**
   ```bash
   python topstepx_auth.py
   ```
4. **Start trading:**
   ```bash
   python unified_market_connector.py --provider topstepx --symbols CON.F.US.ES.H26
   ```

**That's it!** Your environment is configured and ready to use.
