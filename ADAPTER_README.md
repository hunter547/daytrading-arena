# Market Data Adapter System

A flexible, provider-agnostic market data system that supports multiple data sources (Coinbase, TopstepX, and future providers) through a unified adapter interface.

## Overview

The adapter system provides:

- **Abstract Interface**: `MarketDataAdapter` base class for implementing new providers
- **Normalized Data Models**: Common `Quote`, `Trade`, `Candle`, and `DepthLevel` objects
- **Pluggable Architecture**: Easily swap between providers without changing application code
- **Unified Connector**: Single entry point that works with any adapter

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│               Market Data Sources                        │
│  (Coinbase, TopstepX, Binance, etc.)                    │
└──────────────┬────────────────┬─────────────────────────┘
               │                │
               ▼                ▼
    ┌──────────────┐  ┌──────────────┐
    │   Coinbase   │  │   TopstepX   │
    │   Adapter    │  │   Adapter    │
    └──────┬───────┘  └──────┬───────┘
           │                  │
           └────────┬─────────┘
                    ▼
         ┌─────────────────────┐
         │  MarketDataAdapter  │  (Abstract Interface)
         │  - Quote            │
         │  - Trade            │
         │  - Candle           │
         │  - Depth            │
         └─────────┬───────────┘
                   │
                   ▼
         ┌─────────────────────┐
         │  Unified Connector  │
         └─────────┬───────────┘
                   │
                   ▼
              Kafka Broker
                   │
                   ▼
         AgentRouterNode(s) → Trading Agents
```

## Components

### 1. Market Data Adapter (`market_data_adapter.py`)

Abstract base class defining the interface for market data providers.

**Key Classes:**
- `MarketDataAdapter` - Abstract base class
- `Quote` - Best bid/ask prices
- `Trade` - Executed trade data
- `Candle` - OHLCV historical data
- `DepthLevel` - Order book depth

**Key Methods:**
```python
async def connect() -> None
async def disconnect() -> None
async def subscribe(symbols: list[str]) -> None
async def unsubscribe(symbols: list[str]) -> None
async def fetch_candles(...) -> list[Candle]
def normalize_symbol(symbol: str) -> str
```

### 2. Coinbase Adapter (`coinbase_adapter.py`)

Implementation for Coinbase Exchange cryptocurrency markets.

**Features:**
- WebSocket connection via `ticker_batch` channel (~5s updates)
- REST API for historical OHLCV candles
- Automatic reconnection with exponential backoff
- Symbol format: `BTC-USD`, `ETH-USD`, etc.

**Usage:**
```python
from coinbase_adapter import CoinbaseAdapter

adapter = CoinbaseAdapter(
    symbols=["BTC-USD", "ETH-USD", "SOL-USD"],
    on_quote=handle_quote,
)
await adapter.start()
```

### 3. TopstepX Adapter (`topstepx_adapter.py`)

Implementation for TopstepX (ProjectX) CME futures markets.

**Features:**
- SignalR WebSocket connection to market hub
- Real-time quotes, trades, and market depth (DOM)
- REST API for historical bars
- JWT authentication
- Symbol format: `CON.F.US.ES.H26` (contract IDs)

**Prerequisites:**
```bash
pip install signalrcore
```

**Usage:**
```python
from topstepx_adapter import TopstepXAdapter

adapter = TopstepXAdapter(
    jwt_token="your_jwt_token",
    symbols=["CON.F.US.ES.H26", "CON.F.US.NQ.H26"],
    environment="demo",  # or "topstepx", "alpha-ticks", etc.
    on_quote=handle_quote,
    on_trade=handle_trade,
    on_depth=handle_depth,
)
await adapter.start()
```

### 4. Unified Market Connector (`unified_market_connector.py`)

Bridges any adapter to the Kafka-based agent system.

**Features:**
- Works with any `MarketDataAdapter` implementation
- Publishes normalized data to Kafka
- Configurable throttling per symbol
- Automatic candle refresh
- Enriched prompts with multi-timeframe analysis

**Usage:**
```bash
# Coinbase
python unified_market_connector.py \
    --provider coinbase \
    --symbols BTC-USD ETH-USD \
    --interval 5 \
    --candle-interval 60

# TopstepX
TOPSTEPX_JWT_TOKEN=your_token python unified_market_connector.py \
    --provider topstepx \
    --symbols CON.F.US.ES.H26 CON.F.US.NQ.H26 \
    --interval 1
```

## Adding a New Provider

To add support for a new market data provider:

1. **Create adapter class** inheriting from `MarketDataAdapter`:

```python
from market_data_adapter import MarketDataAdapter, Quote, Candle

class MyProviderAdapter(MarketDataAdapter):
    async def connect(self) -> None:
        # Establish connection
        pass
    
    async def subscribe(self, symbols: list[str]) -> None:
        # Subscribe to symbols
        pass
    
    async def fetch_candles(self, symbol, granularity, start, end, limit):
        # Fetch historical data
        return []
    
    def normalize_symbol(self, symbol: str) -> str:
        # Convert to provider format
        return symbol.upper()
```

2. **Implement WebSocket/REST logic**:
   - Handle incoming messages
   - Convert to `Quote`, `Trade`, `Candle`, `DepthLevel` objects
   - Call `self._emit_quote()`, `self._emit_trade()`, etc.

3. **Register in unified connector**:

```python
# In unified_market_connector.py main()
elif args.provider == "myprovider":
    from my_provider_adapter import MyProviderAdapter
    adapter = MyProviderAdapter(symbols=args.symbols)
```

## Data Model Reference

### Quote
```python
@dataclass
class Quote:
    symbol: str
    timestamp: datetime
    last_price: float
    best_bid: float
    best_bid_size: float
    best_ask: float
    best_ask_size: float
    volume_24h: Optional[float] = None
    open_24h: Optional[float] = None
    high_24h: Optional[float] = None
    low_24h: Optional[float] = None
```

### Trade
```python
@dataclass
class Trade:
    symbol: str
    timestamp: datetime
    price: float
    size: float
    side: str  # "buy" or "sell"
    trade_id: Optional[str] = None
```

### Candle
```python
@dataclass
class Candle:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    granularity_seconds: int
```

### DepthLevel
```python
@dataclass
class DepthLevel:
    symbol: str
    timestamp: datetime
    side: str  # "bid" or "ask"
    price: float
    size: float
```

## Environment Variables

### Kafka
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka broker addresses (default: `localhost:9092`)

### TopstepX
- `TOPSTEPX_JWT_TOKEN` - JWT authentication token (required)
- `TOPSTEPX_ENVIRONMENT` - Environment name: `demo`, `topstepx`, `alpha-ticks`, etc. (default: `demo`)

## Migration Guide

### From Old Coinbase Connector

**Before:**
```bash
python coinbase_connector.py --products BTC-USD ETH-USD --interval 30
```

**After:**
```bash
python unified_market_connector.py \
    --provider coinbase \
    --symbols BTC-USD ETH-USD \
    --interval 30
```

### Switching to TopstepX

Simply change the provider and symbols:

```bash
TOPSTEPX_JWT_TOKEN=your_token python unified_market_connector.py \
    --provider topstepx \
    --symbols CON.F.US.ES.H26 \
    --interval 1
```

The rest of your system (agents, tools, dashboard) continues to work unchanged!

## Symbol Format Guide

### Coinbase
- Format: `BASE-QUOTE`
- Examples: `BTC-USD`, `ETH-USD`, `SOL-USD`, `FARTCOIN-USD`

### TopstepX (ProjectX)
- Format: `CON.{Type}.{Region}.{Symbol}.{Contract}`
- Type: `F` (Futures), `O` (Options)
- Region: `US` (United States)
- Symbol: `ES` (E-mini S&P), `NQ` (E-mini NASDAQ), `RTY` (E-mini Russell)
- Contract: `{Month}{Year}` (e.g., `H26` = March 2025, `M25` = June 2025)
- Examples:
  - `CON.F.US.ES.H26` - E-mini S&P March 2025
  - `CON.F.US.NQ.M25` - E-mini NASDAQ June 2025
  - `CON.F.US.RTY.Z24` - E-mini Russell December 2024

**Contract Month Codes:**
- F = January, G = February, H = March, J = April
- K = May, M = June, N = July, Q = August
- U = September, V = October, X = November, Z = December

## Testing

### Test Coinbase Adapter

```python
import asyncio
from coinbase_adapter import CoinbaseAdapter

async def test():
    def on_quote(quote):
        print(f"Quote: {quote.symbol} @ ${quote.last_price}")
    
    adapter = CoinbaseAdapter(
        symbols=["BTC-USD"],
        on_quote=on_quote,
    )
    
    await adapter.start()
    await asyncio.sleep(60)  # Run for 1 minute
    await adapter.stop()

asyncio.run(test())
```

### Test TopstepX Adapter

```python
import asyncio
import os
from topstepx_adapter import TopstepXAdapter

async def test():
    def on_quote(quote):
        print(f"Quote: {quote.symbol} @ ${quote.last_price}")
    
    adapter = TopstepXAdapter(
        jwt_token=os.getenv("TOPSTEPX_JWT_TOKEN"),
        symbols=["CON.F.US.ES.H26"],
        environment="demo",
        on_quote=on_quote,
    )
    
    await adapter.start()
    await asyncio.sleep(60)
    await adapter.stop()

asyncio.run(test())
```

## Troubleshooting

### TopstepX: SignalR Not Found
```bash
pip install signalrcore
```

### TopstepX: Authentication Error
- Verify `TOPSTEPX_JWT_TOKEN` is set correctly
- Check token expiration
- Confirm environment matches your account (`demo`, `topstepx`, etc.)

### Coinbase: WebSocket Disconnects
- Check network connectivity
- Review Coinbase status page
- Adapter automatically reconnects with exponential backoff

### No Data Received
- Verify symbols are correct for the provider
- Check adapter logs for subscription confirmations
- Ensure Kafka broker is running

## Performance Considerations

### Throttling
Use `--interval` to limit publish rate per symbol:
```bash
--interval 5  # Minimum 5 seconds between publishes per symbol
```

### Candle Refresh
Adjust candle fetch frequency:
```bash
--candle-interval 120  # Fetch candles every 2 minutes
```

### Memory Usage
- Adapters maintain minimal state (latest quote per symbol)
- Historical candles are fetched on-demand, not stored
- Consider symbol count and update frequency for resource planning

## Future Providers

Potential adapters to implement:
- **Binance** - Crypto spot and futures
- **Interactive Brokers** - Multi-asset broker
- **Polygon.io** - Stocks, options, forex
- **Alpaca** - Commission-free trading API
- **Historical Replay** - Backtest with recorded data

Each requires ~200-300 lines of adapter code to integrate!
