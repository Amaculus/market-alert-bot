"""
Market Data Model

Simple dataclass for prediction market data.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime


@dataclass
class Market:
    """Represents a prediction market from any platform"""
    id: str
    platform: str  # 'kalshi' or 'polymarket'
    market_id: str  # platform-specific ID
    title: str
    event_id: Optional[str] = None  # NEW: Parent event ID (series_ticker or group_id)
    event_title: Optional[str] = None  # NEW: Parent event title if available
    subtitle: Optional[str] = None
    category: Optional[str] = None
    series_ticker: Optional[str] = None  # Kalshi only (kept for backward compat)
    tags: List[str] = field(default_factory=list)  # Polymarket tags
    current_odds: Dict = field(default_factory=dict)
    volume: float = 0.0
    event_date: Optional[datetime] = None
    status: str = 'active'
    raw_data: Dict = field(default_factory=dict)
    
    def get_full_text(self) -> str:
        """Get combined text for entity extraction"""
        texts = [self.title]
        if self.event_title:
            texts.insert(0, self.event_title)
        if self.subtitle:
            texts.append(self.subtitle)
        return " ".join(texts)