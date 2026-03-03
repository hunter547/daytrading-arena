"""
TopstepX (ProjectX) market data adapter for CME futures.

Implements the MarketDataAdapter interface for TopstepX Gateway API,
providing SignalR WebSocket real-time data and REST API historical data.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from market_data_adapter import (
    Candle,
    DepthLevel,
    MarketDataAdapter,
    Quote,
    Trade,
)

logger = logging.getLogger(__name__)

# TopstepX API endpoints (official URLs)
# API Endpoint: https://api.topstepx.com
# User Hub:     https://rtc.topstepx.com/hubs/user
# Market Hub:   https://rtc.topstepx.com/hubs/market
TOPSTEPX_REST_BASE = "https://api.topstepx.com"
TOPSTEPX_WS_BASE = "https://rtc.topstepx.com"


class TopstepXAdapter(MarketDataAdapter):
    """Market data adapter for TopstepX API.
    
    Features:
    - SignalR WebSocket connection for market hub
    - Real-time quotes, trades, and market depth
    - REST API for historical bars (OHLCV candles)
    - CME futures contract support
    - Authentication via JWT token
    
    Note: This implementation uses a Python SignalR client library.
    You'll need to install: pip install signalrcore
    """
    
    def __init__(
        self,
        jwt_token: str,
        *args,
        environment: str = "demo",
        api_base_url: Optional[str] = None,
        **kwargs
    ):
        """Initialize TopstepX adapter.
        
        Args:
            jwt_token: JWT bearer token for authentication
            environment: Environment name (demo, alpha-ticks, topstepx, etc.)
            api_base_url: Custom API base URL (overrides environment-based URL)
            *args, **kwargs: Passed to parent MarketDataAdapter
        """
        super().__init__(*args, **kwargs)
        self._jwt_token = jwt_token
        self._environment = environment
        self._http_client = httpx.AsyncClient(
            timeout=30.0,
            headers={"Authorization": f"Bearer {jwt_token}"}
        )
        
        # Use TopstepX direct API URLs (official)
        # API: https://api.topstepx.com
        # WebSocket: https://rtc.topstepx.com
        if api_base_url:
            # Custom API URL provided
            self._rest_base = api_base_url.rstrip('/')
            self._ws_base = api_base_url.replace('api.', 'rtc.').rstrip('/')
        else:
            # Use official TopstepX URLs
            self._rest_base = "https://api.topstepx.com"
            self._ws_base = "https://rtc.topstepx.com"
        
        # SignalR connection (initialized in connect())
        self._signalr_connection = None
        self._hub_url = f"{self._ws_base}/hubs/market?access_token={jwt_token}"
    
    def normalize_symbol(self, symbol: str) -> str:
        """Normalize symbol to TopstepX contract ID format.
        
        TopstepX uses contract IDs like: CON.F.US.ES.H26
        - CON = Contract
        - F = Futures
        - US = United States
        - ES = E-mini S&P 500
        - H25 = March 2025
        
        Args:
            symbol: Symbol to normalize (e.g., 'ESH25', 'ES', 'CON.F.US.ES.H26')
            
        Returns:
            Normalized TopstepX contract ID
        """
        # If already in TopstepX format, return as-is
        if symbol.startswith("CON."):
            return symbol
        
        # Otherwise, you'd implement logic to convert common symbols
        # to TopstepX contract IDs. This would require a mapping table
        # or API call to search for contracts.
        # For now, return as-is and let the user provide proper format
        logger.warning(
            f"Symbol '{symbol}' should be in TopstepX contract ID format "
            f"(e.g., 'CON.F.US.ES.H26'). Using as-is."
        )
        return symbol
    
    async def connect(self) -> None:
        """Establish SignalR WebSocket connection to TopstepX market hub."""
        try:
            # Import SignalR client
            from signalrcore.hub_connection_builder import HubConnectionBuilder
            from signalrcore.protocol.messagepack_protocol import MessagePackHubProtocol
            
            logger.info(f"Connecting to TopstepX SignalR: {self._ws_base}")
            
            # Build SignalR connection
            self._signalr_connection = (
                HubConnectionBuilder()
                .with_url(
                    self._hub_url,
                    options={
                        "skip_negotiation": True,
                        "access_token_factory": lambda: self._jwt_token,
                        "headers": {"Authorization": f"Bearer {self._jwt_token}"},
                    }
                )
                .with_automatic_reconnect({
                    "type": "interval",
                    "intervals": [1, 2, 5, 10, 30, 60]
                })
                .build()
            )
            
            # Register event handlers with wrapper functions
            # SignalR passes arguments in a list, need to unpack them
            def quote_wrapper(args):
                logger.info(f"Quote received - Raw args: {args}, Type: {type(args)}, Len: {len(args) if isinstance(args, (list, tuple)) else 'N/A'}")
                if isinstance(args, list) and len(args) >= 2:
                    contract_id, data = args[0], args[1]
                    logger.debug(f"Quote unpacked: {contract_id}, {data}")
                    self._handle_quote(contract_id, data)
                else:
                    logger.warning(f"Unexpected quote args format: {args}")
            
            def trade_wrapper(args):
                logger.info(f"Trade received - Raw args: {args}, Type: {type(args)}, Len: {len(args) if isinstance(args, (list, tuple)) else 'N/A'}")
                if isinstance(args, list) and len(args) >= 2:
                    contract_id, data = args[0], args[1]
                    logger.debug(f"Trade unpacked: {contract_id}, {data}")
                    self._handle_trade(contract_id, data)
                else:
                    logger.warning(f"Unexpected trade args format: {args}")
            
            def depth_wrapper(args):
                logger.info(f"Depth received - Raw args: {args}, Type: {type(args)}, Len: {len(args) if isinstance(args, (list, tuple)) else 'N/A'}")
                if isinstance(args, list) and len(args) >= 2:
                    contract_id, data = args[0], args[1]
                    logger.debug(f"Depth unpacked: {contract_id}, {data}")
                    self._handle_depth(contract_id, data)
                else:
                    logger.warning(f"Unexpected depth args format: {args}")
            
            # Register for TopstepX SignalR events
            self._signalr_connection.on("GatewayQuote", quote_wrapper)
            self._signalr_connection.on("GatewayTrade", trade_wrapper)
            self._signalr_connection.on("GatewayDepth", depth_wrapper)
            
            # Also try without "Gateway" prefix
            self._signalr_connection.on("Quote", quote_wrapper)
            self._signalr_connection.on("Trade", trade_wrapper)
            self._signalr_connection.on("Depth", depth_wrapper)
            
            self._signalr_connection.on_open(lambda: logger.info("SignalR connected"))
            self._signalr_connection.on_close(lambda: logger.info("SignalR disconnected"))
            self._signalr_connection.on_error(lambda data: logger.error(f"SignalR error: {data}"))
            
            # Start connection
            self._signalr_connection.start()
            
            logger.info("TopstepX SignalR connected")
            
        except ImportError:
            logger.error(
                "signalrcore package not installed. "
                "Install with: pip install signalrcore"
            )
            raise
        except Exception as e:
            logger.error(f"Error connecting to TopstepX: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Close SignalR connection."""
        if self._signalr_connection:
            try:
                self._signalr_connection.stop()
                logger.info("TopstepX SignalR disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting: {e}")
        
        await self._http_client.aclose()
    
    async def subscribe(self, symbols: list[str]) -> None:
        """Subscribe to market data for contract IDs.
        
        Args:
            symbols: List of TopstepX contract IDs (e.g., ['CON.F.US.ES.H26'])
        """
        if not self._signalr_connection:
            logger.warning("SignalR not connected, cannot subscribe")
            return
        
        for symbol in symbols:
            contract_id = self.normalize_symbol(symbol)
            
            # Subscribe to quotes, trades, and depth
            # Use .invoke() per official docs (Python library requires list)
            self._signalr_connection.invoke("SubscribeContractQuotes", [contract_id])
            self._signalr_connection.invoke("SubscribeContractTrades", [contract_id])
            self._signalr_connection.invoke("SubscribeContractMarketDepth", [contract_id])
            
            logger.info(f"Subscribed to TopstepX market data: {contract_id}")
    
    async def unsubscribe(self, symbols: list[str]) -> None:
        """Unsubscribe from market data.
        
        Args:
            symbols: List of contract IDs to unsubscribe from
        """
        if not self._signalr_connection:
            return
        
        for symbol in symbols:
            contract_id = self.normalize_symbol(symbol)
            
            # Use .invoke() per official docs (Python library requires list)
            self._signalr_connection.invoke("UnsubscribeContractQuotes", [contract_id])
            self._signalr_connection.invoke("UnsubscribeContractTrades", [contract_id])
            self._signalr_connection.invoke("UnsubscribeContractMarketDepth", [contract_id])
            
            logger.info(f"Unsubscribed from TopstepX market data: {contract_id}")
    
    async def fetch_candles(
        self,
        symbol: str,
        granularity_seconds: int,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = None,
    ) -> list[Candle]:
        """Fetch historical bars from TopstepX REST API.
        
        Args:
            symbol: TopstepX contract ID (e.g., 'CON.F.US.ES.H26')
            granularity_seconds: Candle period in seconds
            start_time: Start time for historical data
            end_time: End time for historical data
            limit: Maximum number of bars (max 20,000)
            
        Returns:
            List of Candle objects in chronological order
        """
        contract_id = self.normalize_symbol(symbol)
        
        # Map granularity_seconds to TopstepX unit/unitNumber
        # TopstepX units: 1=Second, 2=Minute, 3=Hour, 4=Day, 5=Week, 6=Month
        unit, unit_number = self._map_granularity(granularity_seconds)
        
        # Build request payload
        url = f"{self._rest_base}/api/History/retrieveBars"
        payload = {
            "contractId": contract_id,
            "live": False,  # Use sim data subscription
            "startTime": start_time.isoformat(),
            "endTime": end_time.isoformat(),
            "unit": unit,
            "unitNumber": unit_number,
            "limit": limit or 300,
            "includePartialBar": False,
        }
        
        try:
            response = await self._http_client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            
            if not data.get("success"):
                error_msg = data.get("errorMessage", "Unknown error")
                logger.error(f"TopstepX API error: {error_msg}")
                return []
            
            # Parse bars: {t, o, h, l, c, v}
            candles = []
            for bar in data.get("bars", []):
                timestamp = datetime.fromisoformat(bar["t"])
                candles.append(
                    Candle(
                        symbol=symbol,
                        timestamp=timestamp,
                        open=float(bar["o"]),
                        high=float(bar["h"]),
                        low=float(bar["l"]),
                        close=float(bar["c"]),
                        volume=float(bar["v"]),
                        granularity_seconds=granularity_seconds,
                    )
                )
            
            logger.info(f"Fetched {len(candles)} bars for {contract_id}")
            return candles
            
        except Exception as e:
            logger.error(f"Error fetching bars for {contract_id}: {e}")
            return []
    
    def _map_granularity(self, seconds: int) -> tuple[int, int]:
        """Map granularity in seconds to TopstepX unit/unitNumber.
        
        Args:
            seconds: Granularity in seconds
            
        Returns:
            Tuple of (unit, unitNumber)
        """
        if seconds < 60:
            return (1, seconds)  # Seconds
        elif seconds < 3600:
            return (2, seconds // 60)  # Minutes
        elif seconds < 86400:
            return (3, seconds // 3600)  # Hours
        elif seconds < 604800:
            return (4, seconds // 86400)  # Days
        elif seconds < 2592000:
            return (5, seconds // 604800)  # Weeks
        else:
            return (6, seconds // 2592000)  # Months
    
    def _handle_quote(self, contract_id: str, data: dict) -> None:
        """Handle GatewayQuote event from SignalR.
        
        Example payload:
        {
            "symbol": "F.US.ES",
            "symbolName": "/ES",
            "lastPrice": 5000.25,
            "bestBid": 5000.00,
            "bestAsk": 5000.50,
            "change": 25.50,
            "changePercent": 0.51,
            "open": 4990.00,
            "high": 5010.00,
            "low": 4985.00,
            "volume": 12000,
            "lastUpdated": "2024-07-21T13:45:00Z",
            "timestamp": "2024-07-21T13:45:00Z"
        }
        """
        try:
            # Log raw data for debugging
            logger.debug(f"Raw quote data for {contract_id}: {data}")
            
            # Skip quotes that don't have both bid and ask
            if "bestBid" not in data or "bestAsk" not in data:
                logger.debug(f"Skipping incomplete quote (missing bid or ask): {data}")
                return
            
            timestamp = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
            
            # lastPrice may not be present, use midpoint if missing
            best_bid = float(data["bestBid"])
            best_ask = float(data["bestAsk"])
            last_price = float(data.get("lastPrice", (best_bid + best_ask) / 2))
            
            quote = Quote(
                symbol=contract_id,
                timestamp=timestamp,
                last_price=last_price,
                best_bid=best_bid,
                best_bid_size=0.0,  # TopstepX doesn't provide size in quote
                best_ask=best_ask,
                best_ask_size=0.0,
                volume_24h=float(data.get("volume", 0)),
                open_24h=float(data.get("open", 0)),
                high_24h=float(data.get("high", 0)),
                low_24h=float(data.get("low", 0)),
            )
            
            self._emit_quote(quote)
            
        except Exception as e:
            logger.error(f"Error handling quote: {e}")
            logger.error(f"Raw data: {data}")
    
    def _handle_trade(self, contract_id: str, data) -> None:
        """Handle GatewayTrade event from SignalR.
        
        TopstepX sends trade data as a list of trade objects.
        
        Example payload (list):
        [
            {
                "symbolId": "F.US.ES",
                "price": 5000.25,
                "timestamp": "2024-07-21T13:45:00Z",
                "type": 0,  # 0=Buy, 1=Sell
                "volume": 2
            },
            ...
        ]
        """
        try:
            # Log raw data for debugging
            logger.debug(f"Raw trade data for {contract_id}: {data}")
            
            # TopstepX sends a list of trades
            trades_list = data if isinstance(data, list) else [data]
            
            for trade_data in trades_list:
                try:
                    timestamp = datetime.fromisoformat(trade_data["timestamp"].replace("Z", "+00:00"))
                    
                    # Map TradeLogType enum: 0=Buy, 1=Sell
                    side = "buy" if trade_data["type"] == 0 else "sell"
                    
                    trade = Trade(
                        symbol=contract_id,
                        timestamp=timestamp,
                        price=float(trade_data["price"]),
                        size=float(trade_data["volume"]),
                        side=side,
                    )
                    
                    self._emit_trade(trade)
                    
                except Exception as e:
                    logger.debug(f"Error processing trade: {e} - Trade: {trade_data}")
            
        except Exception as e:
            logger.error(f"Error handling trade: {e}")
            logger.error(f"Raw data: {data}")
    
    def _handle_depth(self, contract_id: str, data: list) -> None:
        """Handle GatewayDepth event from SignalR.
        
        TopstepX sends depth data as a list of depth levels.
        
        Example payload (list of dicts):
        [
            {
                "timestamp": "2024-07-21T13:45:00Z",
                "type": 1,  # DomType enum: 1=Ask, 2=Bid, 3=BestAsk, 4=BestBid, 5=Trade, 6=Reset, etc.
                "price": 5000.00,
                "volume": 10,
                "currentVolume": 5
            },
            ...
        ]
        """
        try:
            # TopstepX sends a list of depth updates
            if not isinstance(data, list):
                logger.warning(f"Expected depth data as list, got {type(data)}: {data}")
                return
            
            for level in data:
                try:
                    # Skip if no timestamp or invalid timestamp
                    timestamp_str = level.get("timestamp", "")
                    if not timestamp_str or timestamp_str.startswith("0001-01-01"):
                        # Use current time for missing timestamps
                        timestamp = datetime.now(timezone.utc)
                    else:
                        timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                    
                    # Map DomType enum: 1=Ask, 2=Bid, 3=BestAsk, 4=BestBid, 5=Trade, 6=Reset, etc.
                    dom_type = level.get("type", 0)
                    
                    # Only process bid/ask levels (1=Ask, 2=Bid)
                    if dom_type not in [1, 2]:
                        continue  # Skip other types (BestBid/Ask, Trade, Reset, etc.)
                    
                    side = "ask" if dom_type == 1 else "bid"
                    price = float(level.get("price", 0))
                    size = float(level.get("currentVolume", 0))
                    
                    if price <= 0:
                        continue  # Skip invalid prices
                    
                    depth = DepthLevel(
                        symbol=contract_id,
                        timestamp=timestamp,
                        side=side,
                        price=price,
                        size=size,
                    )
                    
                    self._emit_depth(depth)
                    
                except Exception as e:
                    logger.debug(f"Error processing depth level: {e} - Level: {level}")
                    
        except Exception as e:
            logger.error(f"Error handling depth: {e}")
            logger.error(f"Raw data: {data}")
