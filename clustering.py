"""
Market Clustering Engine - Hash-Based (Fast)

Groups similar markets using keyword hashing instead of 
expensive pairwise string comparisons.
"""

import re
import logging
from typing import List, Dict, Set
from dataclasses import dataclass, field

from market import Market

logger = logging.getLogger(__name__)


@dataclass
class MarketCluster:
    """A group of related markets representing the same event"""
    primary_market: Market
    related_markets: List[Market] = field(default_factory=list)
    
    @property
    def total_volume(self) -> float:
        return self.primary_market.volume + sum(m.volume for m in self.related_markets)
    
    @property
    def platform_spread(self) -> List[str]:
        platforms = {self.primary_market.platform}
        for m in self.related_markets:
            platforms.add(m.platform)
        return list(platforms)
    
    @property
    def title(self) -> str:
        return self.primary_market.title


class ClusteringEngine:
    """Fast hash-based clustering"""
    
    # Common words to ignore when generating keys
    STOP_WORDS = {
        'will', 'the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have',
        'i', 'it', 'for', 'not', 'on', 'with', 'he', 'as', 'you', 'do',
        'at', 'this', 'but', 'his', 'by', 'from', 'they', 'we', 'say',
        'her', 'she', 'or', 'an', 'my', 'one', 'all', 'would', 'there',
        'their', 'what', 'so', 'up', 'out', 'if', 'about', 'who', 'get',
        'which', 'go', 'me', 'when', 'make', 'can', 'like', 'time', 'no',
        'just', 'him', 'know', 'take', 'people', 'into', 'year', 'your',
        'good', 'some', 'could', 'them', 'see', 'other', 'than', 'then',
        'now', 'look', 'only', 'come', 'its', 'over', 'think', 'also',
        'back', 'after', 'use', 'two', 'how', 'our', 'work', 'first',
        'well', 'way', 'even', 'new', 'want', 'because', 'any', 'these',
        'give', 'day', 'most', 'us', 'before', 'during', 'after',
        # Prediction market specific
        'market', 'prediction', 'odds', 'bet', 'betting', 'price', 'yes', 'no',
        'win', 'winner', 'happen', 'become', 'next', 'by', 'end', 'before'
    }
    
    def __init__(self):
        pass
    
    def cluster_markets(self, markets: List[Market]) -> List[MarketCluster]:
        """Group markets by extracted key phrases - O(n) complexity"""
        
        if not markets:
            return []
        
        logger.info(f"Clustering {len(markets)} markets using hash method...")
        
        # Sort by volume descending - highest volume becomes primary
        sorted_markets = sorted(markets, key=lambda x: x.volume, reverse=True)
        
        # Hash map: key -> cluster
        clusters: Dict[str, MarketCluster] = {}
        
        for market in sorted_markets:
            # Generate multiple keys for this market
            keys = self._generate_keys(market.title)
            
            # Check if any key matches existing cluster
            matched_cluster = None
            matched_key = None
            
            for key in keys:
                if key in clusters:
                    matched_cluster = clusters[key]
                    matched_key = key
                    break
            
            if matched_cluster:
                # Add to existing cluster
                matched_cluster.related_markets.append(market)
                # Also register this market's other keys to the same cluster
                for key in keys:
                    if key not in clusters:
                        clusters[key] = matched_cluster
            else:
                # Create new cluster
                new_cluster = MarketCluster(primary_market=market)
                # Register all keys
                for key in keys:
                    clusters[key] = new_cluster
        
        # Deduplicate clusters (multiple keys point to same cluster)
        unique_clusters = list({id(c): c for c in clusters.values()}.values())
        
        logger.info(f"Clustered into {len(unique_clusters)} unique topics")
        
        return unique_clusters
    
    def _generate_keys(self, title: str) -> List[str]:
        """Generate multiple lookup keys from a title"""
        
        # Normalize
        text = title.lower()
        text = re.sub(r'[^a-z0-9\s]', ' ', text)
        
        # Extract words, remove stop words
        words = [w for w in text.split() if w not in self.STOP_WORDS and len(w) > 2]
        
        if not words:
            # Fallback to first few words
            words = text.split()[:3]
        
        keys = []
        
        # Key 1: All significant words sorted (order-independent matching)
        if len(words) >= 2:
            keys.append('_'.join(sorted(words[:5])))
        
        # Key 2: First 3 significant words in order
        if len(words) >= 3:
            keys.append('_'.join(words[:3]))
        
        # Key 3: Named entity style - look for capitalized sequences in original
        entities = re.findall(r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*', title)
        for entity in entities:
            normalized_entity = entity.lower().replace(' ', '_')
            if len(normalized_entity) > 3:
                keys.append(normalized_entity)
        
        # Key 4: Numbers + context (for date-based markets)
        numbers = re.findall(r'\b(20\d{2})\b', title)  # Years
        if numbers and words:
            keys.append(f"{words[0]}_{numbers[0]}")
        
        return keys if keys else [text[:50]]  # Fallback to truncated title