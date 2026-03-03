"""
Coinbase Exchange market data adapter.

Implements the MarketDataAdapter interface for Coinbase Exchange,
providing WebSocket real-time data and REST API historical data.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
import websockets

from market_data_adapter import (
    Candle,
    MarketDataAdapter,
    Quote,
    Trade,
)

logger = logging.getLogger(__name__)

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
COINBASE_REST_BASE = "https://api.exchange.coinbase.com"


class CoinbaseAdapter(MarketDataAdapter):
    """Market data adapter for Coinbase Exchange.
    
    Features:
    - WebSocket connection using ticker_batch channel (~5s updates)
    - Automatic reconnection with exponential backoff
    - REST API for historical OHLCV candles
    - Normalizes Coinbase data to common Quote/Trade/Candle formats
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._http_client = httpx.AsyncClient(timeout=30.0)
        self._reconnect_delay = 1.0
        self._max_reconnect_delay = 60.0
        self._ws_task: Optional[asyncio.Task] = None
    
    def normalize_symbol(self, symbol: str) -> str:
        """Normalize symbol to Coinbase format (e.g., 'BTC-USD').
        
        Args:
            symbol: Symbol to normalize
            
        Returns:
            Normalized symbol in Coinbase format
        """
        # Coinbase uses hyphenated pairs: BTC-USD, ETH-USD, etc.
        return symbol.upper().replace("/", "-")
    
    async def connect(self) -> None:
        """Establish WebSocket connection to Coinbase."""
        logger.info(f"Connecting to Coinbase WebSocket: {COINBASE_WS_URL}")
        
        # Start WebSocket consumer task
        self._ws_task = asyncio.create_task(self._consume_websocket())
    
    async def disconnect(self) -> None:
        """Close WebSocket connection."""
        logger.info("Disconnecting from Coinbase WebSocket")
        
        # Cancel WebSocket task
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        
        # Close WebSocket
        if self._ws:
            await self._ws.close()
            self._ws = None
        
        # Close HTTP client
        await self._http_client.aclose()
    
    async def subscribe(self, symbols: list[str]) -> None:
        """Subscribe to ticker updates for symbols.
        
        Args:
            symbols: List of symbols to subscribe to (e.g., ['BTC-USD', 'ETH-USD'])
        """
        if not self._ws or self._ws.closed:
            logger.warning("WebSocket not connected, cannot subscribe")
            return
        
        normalized = [self.normalize_symbol(s) for s in symbols]
        subscribe_msg = {
            "type": "subscribe",
            "product_ids": normalized,
            "channels": ["ticker_batch"],
        }
        
        await self._ws.send(json.dumps(subscribe_msg))
        logger.info(f"Subscribed to Coinbase tickers: {normalized}")
    
    async def unsubscribe(self, symbols: list[str]) -> None:
        """Unsubscribe from ticker updates.
        
        Args:
            symbols: List of symbols to unsubscribe from
        """
        if not self._ws or self._ws.closed:
            return
        
        normalized = [self.normalize_symbol(s) for s in symbols]
        unsubscribe_msg = {
            "type": "unsubscribe",
            "product_ids": normalized,
            "channels": ["ticker_batch"],
        }
        
        await self._ws.send(json.dumps(unsubscribe_msg))
        logger.info(f"Unsubscribed from Coinbase tickers: {normalized}")
    
    async def fetch_candles(
        self,
        symbol: str,
        granularity_seconds: int,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = None,
    ) -> list[Candle]:
        """Fetch historical OHLCV candles from Coinbase REST API.
        
        Args:
            symbol: Symbol to fetch (e.g., 'BTC-USD')
            granularity_seconds: Candle period (60, 300, 900, 3600, 21600, 86400)
            start_time: Start time for historical data
            end_time: End time for historical data
            limit: Maximum number of candles (Coinbase max: 300)
            
        Returns:
            List of Candle objects in chronological order
        """
        normalized = self.normalize_symbol(symbol)
        
        # Coinbase candles endpoint: GET /products/{product_id}/candles
        # Query params: start, end, granularity
        url = f"{COINBASE_REST_BASE}/products/{normalized}/candles"
        params = {
            "start": start_time.isoformat(),
            "end": end_time.isoformat(),
            "granularity": granularity_seconds,
        }
        
        try:
            response = await self._http_client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Coinbase returns: [[timestamp, low, high, open, close, volume], ...]
            # in reverse chronological order
            candles = []
            for row in reversed(data):  # Reverse to get chronological order
                timestamp, low, high, open_price, close, volume = row
                candles.append(
                    Candle(
                        symbol=symbol,
                        timestamp=datetime.fromtimestamp(timestamp, tz=timezone.utc),
                        open=float(open_price),
                        high=float(high),
                        low=float(low),
                        close=float(close),
                        volume=float(volume),
                        granularity_seconds=granularity_seconds,
                    )
                )
            
            if limit:
                candles = candles[-limit:]
            
            logger.info(f"Fetched {len(candles)} candles for {symbol}")
            return candles
            
        except Exception as e:
            logger.error(f"Error fetching candles for {symbol}: {e}")
            return []
    
    async def _consume_websocket(self) -> None:
        """Consume WebSocket messages with automatic reconnection."""
        while self._running:
            try:
                async with websockets.connect(COINBASE_WS_URL) as ws:
                    self._ws = ws
                    logger.info("Coinbase WebSocket connected")
                    
                    # Reset reconnect delay on successful connection
                    self._reconnect_delay = 1.0
                    
                    # Subscribe to initial symbols
                    await self.subscribe(self._symbols)
                    
                    # Consume messages
                    async for message in ws:
                        await self._handle_message(message)
                        
            except websockets.exceptions.ConnectionClosed:
                logger.warning("Coinbase WebSocket connection closed")
            except Exception as e:
                logger.error(f"Coinbase WebSocket error: {e}")
            
            if not self._running:
                break
            
            # Exponential backoff for reconnection
            logger.info(f"Reconnecting in {self._reconnect_delay}s...")
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * 2,
                self._max_reconnect_delay
            )
    
    async def _handle_message(self, message: str) -> None:
        """Process incoming WebSocket message.
        
        Args:
            message: Raw WebSocket message
        """
        try:
            data = json.loads(message)
            msg_type = data.get("type")
            
            if msg_type == "ticker":
                # ticker_batch channel sends individual ticker updates
                await self._handle_ticker(data)
            elif msg_type == "subscriptions":
                logger.debug(f"Subscriptions confirmed: {data}")
            elif msg_type == "error":
                logger.error(f"Coinbase error: {data.get('message')}")
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")
    
    async def _handle_ticker(self, data: dict) -> None:
        """Process ticker update and emit Quote.
        
        Args:
            data: Ticker data from WebSocket
        """
        try:
            # Parse timestamp
            timestamp_str = data.get("time")
            if timestamp_str:
                # Coinbase sends ISO format: "2024-01-15T12:34:56.789000Z"
                timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            else:
                timestamp = datetime.now(timezone.utc)
            
            # Create Quote object
            quote = Quote(
                symbol=data["product_id"],
                timestamp=timestamp,
                last_price=float(data["price"]),
                best_bid=float(data["best_bid"]),
                best_bid_size=float(data["best_bid_size"]),
                best_ask=float(data["best_ask"]),
                best_ask_size=float(data["best_ask_size"]),
                volume_24h=float(data.get("volume_24h", 0)),
                open_24h=float(data.get("open_24h", 0)) if data.get("open_24h") else None,
                high_24h=float(data.get("high_24h", 0)) if data.get("high_24h") else None,
                low_24h=float(data.get("low_24h", 0)) if data.get("low_24h") else None,
            )
            
            # Emit quote to callback
            self._emit_quote(quote)
            
        except Exception as e:
            logger.error(f"Error processing ticker: {e}")
