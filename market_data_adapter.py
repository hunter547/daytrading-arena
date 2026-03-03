"""
Abstract market data adapter interface for supporting multiple data providers.

This module provides a unified interface for consuming market data from
different sources (Coinbase, TopstepX, etc.) and publishing to Kafka.

The adapter pattern allows seamless switching between data providers
without modifying the rest of the trading system.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class MarketDataType(Enum):
    """Type of market data update."""
    QUOTE = "quote"  # Best bid/ask prices
    TRADE = "trade"  # Executed trades
    CANDLE = "candle"  # OHLCV candles
    DEPTH = "depth"  # Order book depth


@dataclass
class Quote:
    """Normalized quote data (best bid/ask)."""
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
    
    def spread(self) -> float:
        """Calculate bid-ask spread."""
        return self.best_ask - self.best_bid
    
    def mid_price(self) -> float:
        """Calculate mid-market price."""
        return (self.best_bid + self.best_ask) / 2


@dataclass
class Trade:
    """Normalized trade data."""
    symbol: str
    timestamp: datetime
    price: float
    size: float
    side: str  # "buy" or "sell"
    trade_id: Optional[str] = None


@dataclass
class Candle:
    """Normalized OHLCV candle data."""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    granularity_seconds: int  # Candle period in seconds


@dataclass
class DepthLevel:
    """Normalized order book depth level."""
    symbol: str
    timestamp: datetime
    side: str  # "bid" or "ask"
    price: float
    size: float


class MarketDataUpdate(BaseModel):
    """Container for market data updates."""
    data_type: MarketDataType
    symbol: str
    timestamp: str
    data: dict[str, Any]


class MarketDataAdapter(ABC):
    """Abstract base class for market data adapters.
    
    Implementations must provide methods for:
    - Connecting to the data source
    - Subscribing to symbols
    - Normalizing incoming data to common formats
    - Fetching historical candles
    """
    
    def __init__(
        self,
        symbols: list[str],
        on_quote: Optional[Callable[[Quote], None]] = None,
        on_trade: Optional[Callable[[Trade], None]] = None,
        on_candle: Optional[Callable[[Candle], None]] = None,
        on_depth: Optional[Callable[[DepthLevel], None]] = None,
    ):
        """Initialize the adapter.
        
        Args:
            symbols: List of symbols to subscribe to
            on_quote: Callback for quote updates
            on_trade: Callback for trade updates
            on_candle: Callback for candle updates
            on_depth: Callback for depth updates
        """
        self._symbols = symbols
        self._on_quote = on_quote
        self._on_trade = on_trade
        self._on_candle = on_candle
        self._on_depth = on_depth
        self._running = False
    
    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the data source."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the data source."""
        pass
    
    @abstractmethod
    async def subscribe(self, symbols: list[str]) -> None:
        """Subscribe to market data for the given symbols.
        
        Args:
            symbols: List of symbols to subscribe to
        """
        pass
    
    @abstractmethod
    async def unsubscribe(self, symbols: list[str]) -> None:
        """Unsubscribe from market data for the given symbols.
        
        Args:
            symbols: List of symbols to unsubscribe from
        """
        pass
    
    @abstractmethod
    async def fetch_candles(
        self,
        symbol: str,
        granularity_seconds: int,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = None,
    ) -> list[Candle]:
        """Fetch historical candles for a symbol.
        
        Args:
            symbol: Symbol to fetch candles for
            granularity_seconds: Candle period in seconds (60, 300, 900, etc.)
            start_time: Start time for historical data
            end_time: End time for historical data
            limit: Maximum number of candles to fetch
            
        Returns:
            List of Candle objects
        """
        pass
    
    @abstractmethod
    def normalize_symbol(self, symbol: str) -> str:
        """Normalize a symbol to the adapter's format.
        
        Args:
            symbol: Symbol to normalize
            
        Returns:
            Normalized symbol string
        """
        pass
    
    async def start(self) -> None:
        """Start the adapter and begin consuming data."""
        if self._running:
            logger.warning("Adapter already running")
            return
        
        self._running = True
        await self.connect()
        await self.subscribe(self._symbols)
        logger.info(f"Adapter started for symbols: {self._symbols}")
    
    async def stop(self) -> None:
        """Stop the adapter and close connections."""
        if not self._running:
            return
        
        self._running = False
        await self.unsubscribe(self._symbols)
        await self.disconnect()
        logger.info("Adapter stopped")
    
    def _emit_quote(self, quote: Quote) -> None:
        """Emit a quote update to the callback."""
        if self._on_quote:
            self._on_quote(quote)
    
    def _emit_trade(self, trade: Trade) -> None:
        """Emit a trade update to the callback."""
        if self._on_trade:
            self._on_trade(trade)
    
    def _emit_candle(self, candle: Candle) -> None:
        """Emit a candle update to the callback."""
        if self._on_candle:
            self._on_candle(candle)
    
    def _emit_depth(self, depth: DepthLevel) -> None:
        """Emit a depth update to the callback."""
        if self._on_depth:
            self._on_depth(depth)
