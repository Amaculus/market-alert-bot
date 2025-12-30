"""
API Clients for Kalshi and Polymarket

Fetches market data from prediction market platforms and converts
to our unified Market format for clustering.
"""

import requests
import time
import concurrent.futures
from typing import List, Dict, Optional
from datetime import datetime
from market import Market


class KalshiClient:
    """Client for Kalshi API"""
    
    BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
    
    def __init__(self, email: Optional[str] = None, password: Optional[str] = None):
        self.session = requests.Session()
        self.token = None
        
        if email and password:
            self._login(email, password)
    
    def _login(self, email: str, password: str) -> None:
        try:
            response = self.session.post(
                f"{self.BASE_URL}/login",
                json={"email": email, "password": password}
            )
            response.raise_for_status()
            self.token = response.json().get("token")
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})
        except Exception as e:
            print(f"Kalshi Login Failed: {e}", flush=True)
    
    def get_all_markets(
        self,
        status: str = "open",
        limit: int = 10000
    ) -> List[Market]:
        """Fetch all markets from Kalshi"""
        markets = []
        cursor = None
        
        print(f"Fetching Kalshi markets (limit: {limit})...", flush=True)
        
        while True:
            batch_size = 200
            remaining = limit - len(markets)
            
            if remaining <= 0:
                break
                
            params = {
                "limit": min(remaining, batch_size),
                "status": status
            }
            
            if cursor:
                params["cursor"] = cursor
            
            success = False
            for attempt in range(5):
                try:
                    response = self.session.get(
                        f"{self.BASE_URL}/markets",
                        params=params
                    )
                    
                    if response.status_code == 429:
                        wait_time = 2 ** attempt
                        print(f"  Kalshi Rate Limit hit. Sleeping {wait_time}s...", flush=True)
                        time.sleep(wait_time)
                        continue
                    
                    response.raise_for_status()
                    data = response.json()
                    success = True
                    break
                    
                except requests.exceptions.RequestException as e:
                    print(f"  Error fetching Kalshi page: {e}", flush=True)
                    break
            
            if not success:
                print("  Failed to fetch Kalshi page after retries. Stopping.", flush=True)
                break

            batch = data.get("markets", [])
            if not batch:
                break
                
            for market_data in batch:
                market = self._parse_market(market_data)
                markets.append(market)
            
            if len(markets) % 1000 == 0:
                print(f"  Fetched {len(markets)} Kalshi markets so far...", flush=True)

            cursor = data.get("cursor")
            if not cursor:
                break
                
            time.sleep(0.2)
        
        print(f"Fetched {len(markets)} markets from Kalshi", flush=True)
        return markets
    
    def _parse_market(self, data: Dict) -> Market:
        event_date = None
        if data.get("expiration_time"):
            try:
                event_date = datetime.fromisoformat(data["expiration_time"].replace("Z", "+00:00"))
            except: pass
        
        current_odds = {}
        if data.get("yes_bid") is not None and data.get("no_bid") is not None:
            yes_price = (data.get("yes_bid", 0) + data.get("yes_ask", 0)) / 2
            no_price = (data.get("no_bid", 0) + data.get("no_ask", 0)) / 2
            current_odds = {
                "yes": yes_price / 100,
                "no": no_price / 100
            }
        
        # Extract event_id from series_ticker
        series_ticker = data.get("series_ticker")
        event_id = f"kalshi_{series_ticker}" if series_ticker else f"kalshi_{data['ticker']}"
        
        # Try to get event title from the series title or event title field
        event_title = data.get("event_title") or data.get("series_title")
        
        return Market(
            id=f"kalshi_{data['ticker']}",
            platform="kalshi",
            market_id=data["ticker"],
            title=data.get("title", ""),
            event_id=event_id,
            event_title=event_title,
            subtitle=data.get("subtitle"),
            category=data.get("category"),
            series_ticker=series_ticker,
            tags=[],
            current_odds=current_odds,
            volume=data.get("volume", 0),
            event_date=event_date,
            status=data.get("status", "active"),
            raw_data=data
        )


class PolymarketClient:
    """Client for Polymarket API"""
    
    GAMMA_URL = "https://gamma-api.polymarket.com"
    PAGE_SIZE = 100
    
    def __init__(self):
        self.session = requests.Session()

    def _fetch_page(self, offset: int, active: bool, min_volume: float = None) -> List[Market]:
        """Fetches a single page of markets from Polymarket"""
        params = {
            "limit": self.PAGE_SIZE,
            "offset": offset,
            "archived": "false" if active else "true"
        }

        # Add volume filter if specified
        if min_volume is not None:
            params["volume_num_min"] = min_volume
        
        for attempt in range(3):
            try:
                response = self.session.get(f"{self.GAMMA_URL}/markets", params=params)
                
                if response.status_code == 429:
                    time.sleep(2)
                    continue
                
                response.raise_for_status()
                data = response.json()
                
                markets = []
                for market_data in data:
                    market = self._parse_market(market_data)
                    if market:
                        markets.append(market)
                
                return markets, len(data) < self.PAGE_SIZE 
                
            except requests.exceptions.RequestException as e:
                if attempt == 2:
                    print(f"  Error fetching Polymarket page at offset {offset}: {e}", flush=True)
                time.sleep(1)
        return [], True

    def get_all_markets(
        self,
        active: bool = True,
        limit: int = 10000,
        min_volume: float = None
    ) -> List[Market]:
        """Fetch all markets from Polymarket using parallel pagination"""

        volume_msg = f" (min volume: ${min_volume:,.0f})" if min_volume else ""
        print(f"Fetching Polymarket markets (limit: {limit}){volume_msg} using parallel pages...", flush=True)

        all_markets = []
        initial_pages = 15
        offsets_to_fetch = [i * self.PAGE_SIZE for i in range(initial_pages)]
        max_workers = 3

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_offset = {executor.submit(self._fetch_page, offset, active, min_volume): offset for offset in offsets_to_fetch}
            
            current_offset = initial_pages * self.PAGE_SIZE
            last_page_found = False
            
            while future_to_offset and len(all_markets) < limit:
                done, _ = concurrent.futures.wait(future_to_offset, return_when=concurrent.futures.FIRST_COMPLETED)
                
                for future in done:
                    offset = future_to_offset.pop(future)
                    
                    try:
                        markets_batch, is_last_page = future.result()
                        all_markets.extend(markets_batch)
                        
                        if len(all_markets) % 1000 < len(markets_batch):
                            print(f"  Fetched {len(all_markets)} Polymarket markets so far...", flush=True)
                        
                        if is_last_page:
                            last_page_found = True
                        
                        if not last_page_found and len(all_markets) < limit:
                            next_offset = current_offset
                            current_offset += self.PAGE_SIZE

                            next_future = executor.submit(self._fetch_page, next_offset, active, min_volume)
                            future_to_offset[next_future] = next_offset
                            
                    except Exception as e:
                        print(f"  Polymarket parallel fetch error at offset {offset}: {e}", flush=True)
                
                if last_page_found and not future_to_offset:
                    break

        print(f"Fetched {len(all_markets)} markets from Polymarket", flush=True)
        return all_markets
    
    def _parse_market(self, data: Dict) -> Optional[Market]:
        if not data.get("question"):
            return None
        
        event_date = None
        if data.get("end_date_iso"):
            try:
                event_date = datetime.fromisoformat(data["end_date_iso"].replace("Z", "+00:00"))
            except: pass
        
        current_odds = {}
        if data.get("outcomes") and len(data["outcomes"]) == 2:
            try:
                prices = data.get("outcomePrices", ["0.5", "0.5"])
                current_odds = {
                    "yes": float(prices[0]),
                    "no": float(prices[1])
                }
            except: pass
        
        tags = data.get("tags", [])
        volume = 0
        if data.get("volume"):
            try:
                volume = float(data["volume"])
            except: pass
        
        # Extract event_id - try multiple fields
        # Polymarket uses: groupId, eventId, or slug for grouping
        group_id = (
            data.get("groupId") or 
            data.get("group_id") or 
            data.get("eventId") or 
            data.get("event_id") or
            data.get("slug") or
            data.get("condition_id", data.get("id"))
        )
        event_id = f"polymarket_{group_id}"
        
        # Get event title if available
        event_title = data.get("groupTitle") or data.get("eventTitle") or data.get("group_title")
        
        return Market(
            id=f"polymarket_{data.get('condition_id', data.get('id'))}",
            platform="polymarket",
            market_id=data.get("condition_id", data.get("id")),
            title=data.get("question", ""),
            event_id=event_id,
            event_title=event_title,
            subtitle=data.get("description"),
            category=tags[0] if tags else None,
            series_ticker=None,
            tags=tags,
            current_odds=current_odds,
            volume=volume,
            event_date=event_date,
            status="active" if data.get("active") else "closed",
            raw_data=data
        )


class MarketAggregator:
    """Aggregates markets from multiple platforms"""
    
    def __init__(self, kalshi_email: Optional[str] = None, kalshi_password: Optional[str] = None):
        self.kalshi = KalshiClient(kalshi_email, kalshi_password)
        self.polymarket = PolymarketClient()
    
    def fetch_all_markets(
        self,
        include_kalshi: bool = True,
        include_polymarket: bool = True,
        kalshi_status: str = "open",
        polymarket_active: bool = True,
        min_volume: float = None
    ) -> List[Market]:
        """Fetch markets from all enabled platforms IN PARALLEL"""
        all_markets = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = {}

            if include_kalshi:
                print("Starting Kalshi fetch...", flush=True)
                future_k = executor.submit(self.kalshi.get_all_markets, status=kalshi_status)
                futures[future_k] = "Kalshi"

            if include_polymarket:
                print("Starting Polymarket fetch...", flush=True)
                # Polymarket supports volume filtering at API level
                future_p = executor.submit(self.polymarket.get_all_markets, active=polymarket_active, min_volume=min_volume)
                futures[future_p] = "Polymarket"
            
            for future in concurrent.futures.as_completed(futures):
                platform_name = futures[future]
                try:
                    markets = future.result()
                    all_markets.extend(markets)
                    print(f"Finished fetching {platform_name}", flush=True)
                except Exception as e:
                    print(f"Error fetching {platform_name}: {e}", flush=True)
        
        print(f"\nTotal markets fetched: {len(all_markets)}", flush=True)
        return all_markets


if __name__ == "__main__":
    aggregator = MarketAggregator()
    markets = aggregator.fetch_all_markets()