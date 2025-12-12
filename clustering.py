"""
Market Clustering Engine

Groups similar markets from different platforms (Kalshi/Polymarket)
to create a unified view of a topic and aggregate volume.
"""

from typing import List, Dict, Optional
from dataclasses import dataclass, field
from difflib import SequenceMatcher
import logging

from market import Market

logger = logging.getLogger(__name__)

@dataclass
class MarketCluster:
    """A group of related markets representing the same event"""
    primary_market: Market
    related_markets: List[Market] = field(default_factory=list)
    
    @property
    def total_volume(self) -> float:
        """Combined volume of all markets in cluster"""
        return self.primary_market.volume + sum(m.volume for m in self.related_markets)
    
    @property
    def platform_spread(self) -> List[str]:
        """Which platforms are covering this topic"""
        platforms = {self.primary_market.platform}
        for m in self.related_markets:
            platforms.add(m.platform)
        return list(platforms)
    
    @property
    def title(self) -> str:
        return self.primary_market.title

class ClusteringEngine:
    
    def __init__(self, similarity_threshold: float = 0.85):
        self.threshold = similarity_threshold

    def cluster_markets(self, markets: List[Market]) -> List[MarketCluster]:
        """
        Group markets into clusters based on title similarity.
        Optimization: We assume markets are already filtered by minimum volume before reaching here.
        """
        # Sort by volume desc (highest volume market becomes the "primary" leader)
        sorted_markets = sorted(markets, key=lambda x: x.volume, reverse=True)
        
        clusters: List[MarketCluster] = []
        
        for market in sorted_markets:
            matched = False
            
            # Try to find an existing cluster
            for cluster in clusters:
                # Check Title Similarity
                if self._are_titles_similar(market.title, cluster.primary_market.title):
                    cluster.related_markets.append(market)
                    matched = True
                    break
            
            # If no match, start a new cluster
            if not matched:
                clusters.append(MarketCluster(primary_market=market))
                
        return clusters
    
    def _are_titles_similar(self, title1: str, title2: str) -> bool:
        """Check if two titles represent the same question"""
        # Normalize
        t1 = self._normalize(title1)
        t2 = self._normalize(title2)
        
        # Quick substring check
        if t1 in t2 or t2 in t1:
            return True
            
        # Expensive sequence matcher
        ratio = SequenceMatcher(None, t1, t2).ratio()
        return ratio > self.threshold

    def _normalize(self, text: str) -> str:
        """Clean text for comparison"""
        return text.lower().replace("will", "").replace("?", "").strip()