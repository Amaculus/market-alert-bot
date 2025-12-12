"""
API Clients for Kalshi and Polymarket

Fetches market data from prediction market platforms and converts
to our unified Market format for clustering.
"""

import requests
from typing import List, Dict, Optional
from datetime import datetime
from market import Market


class KalshiClient:
    """Client for Kalshi API"""
    
    BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
    
    def __init__(self, email: Optional[str] = None, password: Optional[str] = None):
        """
        Initialize Kalshi client
        
        Args:
            email: Kalshi account email (optional, for authenticated requests)
            password: Kalshi account password (optional)
        """
        self.session = requests.Session()
        self.token = None
        
        if email and password:
            self._login(email, password)
    
    def _login(self, email: str, password: str) -> None:
        """Authenticate with Kalshi API"""
        response = self.session.post(
            f"{self.BASE_URL}/login",
            json={"email": email, "password": password}
        )
        response.raise_for_status()
        self.token = response.json().get("token")
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})
    
    def get_all_markets(
        self, 
        status: str = "open",
        limit: int = 1000
    ) -> List[Market]:
        """
        Fetch all markets from Kalshi
        
        Args:
            status: Market status filter ('open', 'closed', 'settled')
            limit: Maximum number of markets to fetch
            
        Returns:
            List of Market objects
        """
        markets = []
        cursor = None
        
        while True:
            params = {
                "limit": min(limit - len(markets), 200),  # API limit is 200
                "status": status
            }
            
            if cursor:
                params["cursor"] = cursor
            
            response = self.session.get(
                f"{self.BASE_URL}/markets",
                params=params
            )
            response.raise_for_status()
            data = response.json()
            
            # Process markets
            for market_data in data.get("markets", []):
                market = self._parse_market(market_data)
                markets.append(market)
            
            # Check if there are more results
            cursor = data.get("cursor")
            if not cursor or len(markets) >= limit:
                break
        
        print(f"Fetched {len(markets)} markets from Kalshi")
        return markets
    
    def _parse_market(self, data: Dict) -> Market:
        """Convert Kalshi market data to our Market format"""
        
        # Parse event date
        event_date = None
        if data.get("expiration_time"):
            event_date = datetime.fromisoformat(
                data["expiration_time"].replace("Z", "+00:00")
            )
        
        # Calculate current odds
        current_odds = {}
        if data.get("yes_bid") is not None and data.get("no_bid") is not None:
            yes_price = (data.get("yes_bid", 0) + data.get("yes_ask", 0)) / 2
            no_price = (data.get("no_bid", 0) + data.get("no_ask", 0)) / 2
            current_odds = {
                "yes": yes_price / 100,  # Convert cents to probability
                "no": no_price / 100
            }
        
        return Market(
            id=f"kalshi_{data['ticker']}",
            platform="kalshi",
            market_id=data["ticker"],
            title=data.get("title", ""),
            subtitle=data.get("subtitle"),
            category=data.get("category"),
            series_ticker=data.get("series_ticker"),
            tags=[],
            current_odds=current_odds,
            volume=data.get("volume", 0),
            event_date=event_date,
            status=data.get("status", "active"),
            raw_data=data
        )
    
    def get_markets_by_series(self, series_ticker: str) -> List[Market]:
        """Get all markets in a specific series"""
        response = self.session.get(
            f"{self.BASE_URL}/markets",
            params={"series_ticker": series_ticker}
        )
        response.raise_for_status()
        data = response.json()
        
        return [self._parse_market(m) for m in data.get("markets", [])]


class PolymarketClient:
    """Client for Polymarket API"""
    
    # Polymarket uses CLOB (Central Limit Order Book) API
    BASE_URL = "https://clob.polymarket.com"
    GAMMA_URL = "https://gamma-api.polymarket.com"
    
    def __init__(self):
        """Initialize Polymarket client"""
        self.session = requests.Session()
    
    def get_all_markets(
        self,
        active: bool = True,
        closed: bool = False,
        limit: int = 1000
    ) -> List[Market]:
        """
        Fetch all markets from Polymarket
        
        Args:
            active: Include active markets
            closed: Include closed markets
            limit: Maximum number of markets to fetch
            
        Returns:
            List of Market objects
        """
        markets = []
        offset = 0
        page_size = 100
        
        while len(markets) < limit:
            # Use Gamma API for market data
            params = {
                "limit": page_size,
                "offset": offset,
                "archived": "false" if active else "true"
            }
            
            response = self.session.get(
                f"{self.GAMMA_URL}/markets",
                params=params
            )
            
            if response.status_code != 200:
                print(f"Error fetching Polymarket markets: {response.status_code}")
                break
            
            data = response.json()
            
            if not data:
                break
            
            for market_data in data:
                market = self._parse_market(market_data)
                if market:
                    markets.append(market)
            
            if len(data) < page_size:
                break
            
            offset += page_size
        
        print(f"Fetched {len(markets)} markets from Polymarket")
        return markets
    
    def _parse_market(self, data: Dict) -> Optional[Market]:
        """Convert Polymarket market data to our Market format"""
        
        # Skip if no question
        if not data.get("question"):
            return None
        
        # Parse event date
        event_date = None
        if data.get("end_date_iso"):
            try:
                event_date = datetime.fromisoformat(
                    data["end_date_iso"].replace("Z", "+00:00")
                )
            except:
                pass
        
        # Parse odds - Polymarket uses different formats
        current_odds = {}
        
        # Try to get outcome prices
        if data.get("outcomes"):
            outcomes = data["outcomes"]
            if len(outcomes) == 2:  # Binary market
                # Prices are in the format "0.xx" representing probability
                current_odds = {
                    "yes": float(data.get("outcomePrices", ["0.5", "0.5"])[0]),
                    "no": float(data.get("outcomePrices", ["0.5", "0.5"])[1])
                }
        
        # Extract tags
        tags = []
        if data.get("tags"):
            tags = data["tags"]
        
        # Get volume (convert from string if needed)
        volume = 0
        if data.get("volume"):
            try:
                volume = float(data["volume"])
            except:
                pass
        
        return Market(
            id=f"polymarket_{data.get('condition_id', data.get('id'))}",
            platform="polymarket",
            market_id=data.get("condition_id", data.get("id")),
            title=data.get("question", ""),
            subtitle=data.get("description"),
            category=tags[0] if tags else None,
            series_ticker=None,  # Polymarket doesn't have series tickers
            tags=tags,
            current_odds=current_odds,
            volume=volume,
            event_date=event_date,
            status="active" if data.get("active") else "closed",
            raw_data=data
        )
    
    def search_markets(self, query: str) -> List[Market]:
        """Search for markets by keyword"""
        response = self.session.get(
            f"{self.GAMMA_URL}/markets",
            params={"search": query}
        )
        
        if response.status_code != 200:
            return []
        
        data = response.json()
        return [self._parse_market(m) for m in data if self._parse_market(m)]


class MarketAggregator:
    """Aggregates markets from multiple platforms"""
    
    def __init__(
        self,
        kalshi_email: Optional[str] = None,
        kalshi_password: Optional[str] = None
    ):
        """
        Initialize market aggregator
        
        Args:
            kalshi_email: Kalshi account email (optional)
            kalshi_password: Kalshi account password (optional)
        """
        self.kalshi = KalshiClient(kalshi_email, kalshi_password)
        self.polymarket = PolymarketClient()
    
    def fetch_all_markets(
        self,
        include_kalshi: bool = True,
        include_polymarket: bool = True,
        kalshi_status: str = "open",
        polymarket_active: bool = True
    ) -> List[Market]:
        """
        Fetch markets from all enabled platforms
        
        Args:
            include_kalshi: Fetch from Kalshi
            include_polymarket: Fetch from Polymarket
            kalshi_status: Status filter for Kalshi markets
            polymarket_active: Only active Polymarket markets
            
        Returns:
            Combined list of markets from all platforms
        """
        all_markets = []
        
        if include_kalshi:
            try:
                kalshi_markets = self.kalshi.get_all_markets(status=kalshi_status)
                all_markets.extend(kalshi_markets)
            except Exception as e:
                print(f"Error fetching Kalshi markets: {e}")
        
        if include_polymarket:
            try:
                poly_markets = self.polymarket.get_all_markets(active=polymarket_active)
                all_markets.extend(poly_markets)
            except Exception as e:
                print(f"Error fetching Polymarket markets: {e}")
        
        print(f"\nTotal markets fetched: {len(all_markets)}")
        print(f"  Kalshi: {len([m for m in all_markets if m.platform == 'kalshi'])}")
        print(f"  Polymarket: {len([m for m in all_markets if m.platform == 'polymarket'])}")
        
        return all_markets


# Example usage
if __name__ == "__main__":
    # Initialize aggregator
    aggregator = MarketAggregator()
    
    # Fetch markets
    print("Fetching markets from APIs...")
    markets = aggregator.fetch_all_markets()
    
    # Display sample markets
    print("\n" + "="*60)
    print("SAMPLE MARKETS")
    print("="*60)
    
    for market in markets[:5]:
        print(f"\n{market.platform.upper()}: {market.title}")
        print(f"  ID: {market.market_id}")
        print(f"  Series: {market.series_ticker}")
        print(f"  Odds: {market.current_odds}")
        print(f"  Volume: ${market.volume:,.0f}")
        print(f"  Category: {market.category}")
        print(f"  Tags: {market.tags}")