"""
Market Clustering Engine - Conservative Hash-Based with Parallel Support
"""

import re
import logging
from typing import List, Dict, Set
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor

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
    """Conservative hash-based clustering with parallel support"""
    
    # Words to completely ignore when generating keys
    STOP_WORDS = {
        # Common verbs
        'will', 'be', 'is', 'are', 'was', 'were', 'been', 'being',
        'have', 'has', 'had', 'do', 'does', 'did', 'doing',
        'can', 'could', 'would', 'should', 'may', 'might', 'must',
        'get', 'got', 'make', 'made', 'go', 'going', 'gone',
        'win', 'wins', 'won', 'lose', 'lost', 'beat', 'defeat',
        'become', 'becomes', 'happen', 'happens',
        
        # Articles/Pronouns
        'the', 'a', 'an', 'this', 'that', 'these', 'those',
        'i', 'you', 'he', 'she', 'it', 'we', 'they',
        'my', 'your', 'his', 'her', 'its', 'our', 'their',
        'who', 'what', 'which', 'where', 'when', 'why', 'how',
        
        # Prepositions
        'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by',
        'from', 'up', 'down', 'out', 'off', 'over', 'under',
        'about', 'into', 'through', 'during', 'before', 'after',
        'above', 'below', 'between', 'against', 'until', 'unless',
        
        # Conjunctions
        'and', 'or', 'but', 'if', 'then', 'than', 'so', 'as',
        
        # Common adjectives
        'next', 'first', 'last', 'new', 'old', 'best', 'top',
        'most', 'more', 'any', 'all', 'some', 'other', 'each',
        
        # Prediction market specific
        'market', 'prediction', 'odds', 'bet', 'betting', 'price',
        'yes', 'no', 'winner', 'lead', 'leading',
        
        # Time words (these cause false clusters)
        'january', 'february', 'march', 'april', 'may', 'june',
        'july', 'august', 'september', 'october', 'november', 'december',
        'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
        'today', 'tomorrow', 'yesterday', 'week', 'month', 'year',
        'end', 'start', 'begin', 'beginning', 'ending',
    }
    
    # Years cause too many false matches
    YEAR_PATTERN = re.compile(r'\b20\d{2}\b')
    
    def __init__(self):
        # With 8GB RAM, we can use parallel processing
        self.max_workers = 4
    
    def cluster_markets(self, markets: List[Market]) -> List[MarketCluster]:
        """Group markets by specific entity/event matching"""
        
        if not markets:
            return []
        
        logger.info(f"Clustering {len(markets)} markets...")
        
        # Sort by volume descending
        sorted_markets = sorted(markets, key=lambda x: x.volume, reverse=True)
        
        # For large datasets, use parallel key generation
        if len(sorted_markets) > 5000 and self.max_workers > 1:
            return self._cluster_parallel(sorted_markets)
        
        return self._cluster_serial(sorted_markets)
    
    def _cluster_serial(self, sorted_markets: List[Market]) -> List[MarketCluster]:
        """Serial clustering for smaller datasets"""
        clusters: Dict[str, MarketCluster] = {}
        clustered_market_ids: Set[str] = set()
        
        for market in sorted_markets:
            if market.id in clustered_market_ids:
                continue
            
            keys = self._generate_keys(market.title)
            
            if not keys:
                clusters[f"standalone_{market.id}"] = MarketCluster(primary_market=market)
                clustered_market_ids.add(market.id)
                continue
            
            matched_cluster = None
            for key in keys:
                if key in clusters:
                    matched_cluster = clusters[key]
                    break
            
            if matched_cluster:
                if self._are_truly_related(market.title, matched_cluster.primary_market.title):
                    matched_cluster.related_markets.append(market)
                    clustered_market_ids.add(market.id)
                    for key in keys:
                        if key not in clusters:
                            clusters[key] = matched_cluster
                else:
                    new_cluster = MarketCluster(primary_market=market)
                    for key in keys:
                        if key not in clusters:
                            clusters[key] = new_cluster
                    clustered_market_ids.add(market.id)
            else:
                new_cluster = MarketCluster(primary_market=market)
                for key in keys:
                    clusters[key] = new_cluster
                clustered_market_ids.add(market.id)
        
        unique_clusters = list({id(c): c for c in clusters.values()}.values())
        logger.info(f"Clustered into {len(unique_clusters)} unique topics (serial)")
        return unique_clusters
    
    def _cluster_parallel(self, sorted_markets: List[Market]) -> List[MarketCluster]:
        """Parallel key generation, then serial clustering"""
        
        logger.info(f"Using parallel key generation with {self.max_workers} workers...")
        
        # Parallel: Generate keys for all markets
        def generate_keys_for_market(market):
            return (market, self._generate_keys(market.title))
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = list(executor.map(generate_keys_for_market, sorted_markets))
        
        logger.info(f"Key generation complete, building clusters...")
        
        # Serial: Build clusters (must be serial due to shared state)
        clusters: Dict[str, MarketCluster] = {}
        clustered_market_ids: Set[str] = set()
        
        for market, keys in results:
            if market.id in clustered_market_ids:
                continue
            
            if not keys:
                clusters[f"standalone_{market.id}"] = MarketCluster(primary_market=market)
                clustered_market_ids.add(market.id)
                continue
            
            matched_cluster = None
            for key in keys:
                if key in clusters:
                    matched_cluster = clusters[key]
                    break
            
            if matched_cluster:
                if self._are_truly_related(market.title, matched_cluster.primary_market.title):
                    matched_cluster.related_markets.append(market)
                    clustered_market_ids.add(market.id)
                    for key in keys:
                        if key not in clusters:
                            clusters[key] = matched_cluster
                else:
                    new_cluster = MarketCluster(primary_market=market)
                    for key in keys:
                        if key not in clusters:
                            clusters[key] = new_cluster
                    clustered_market_ids.add(market.id)
            else:
                new_cluster = MarketCluster(primary_market=market)
                for key in keys:
                    clusters[key] = new_cluster
                clustered_market_ids.add(market.id)
        
        unique_clusters = list({id(c): c for c in clusters.values()}.values())
        logger.info(f"Clustered into {len(unique_clusters)} unique topics (parallel)")
        return unique_clusters
    
    def _generate_keys(self, title: str) -> List[str]:
        """Generate specific lookup keys - must have named entities"""
        
        # Remove years (they cause false matches)
        text = self.YEAR_PATTERN.sub('', title)
        
        # Normalize
        text_lower = text.lower()
        text_clean = re.sub(r'[^a-z0-9\s]', ' ', text_lower)
        
        # Extract words, remove stop words
        words = [w for w in text_clean.split() if w not in self.STOP_WORDS and len(w) > 2]
        
        keys = []
        
        # Key 1: Look for proper nouns (capitalized words in original)
        proper_nouns = re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b', title)
        proper_nouns = [p.lower().replace(' ', '_') for p in proper_nouns if len(p) > 3]
        
        # Filter out common words that happen to be capitalized
        proper_nouns = [p for p in proper_nouns if p not in self.STOP_WORDS]
        
        if proper_nouns:
            # Use the longest proper noun phrase as primary key
            longest = max(proper_nouns, key=len)
            if len(longest) > 4:
                keys.append(longest)
            
            # If multiple proper nouns, combine first two
            if len(proper_nouns) >= 2:
                combo = f"{proper_nouns[0]}_{proper_nouns[1]}"
                keys.append(combo)
        
        # Key 2: First 3-4 significant words (if no proper nouns found)
        if not keys and len(words) >= 3:
            # Only use if words are specific enough
            specific_words = [w for w in words if len(w) >= 4]
            if len(specific_words) >= 2:
                keys.append('_'.join(specific_words[:3]))
        
        # Key 3: Look for team vs team pattern (e.g., "Lakers vs Celtics")
        vs_match = re.search(r'(\w+)\s+(?:vs\.?|versus|v\.?|@)\s+(\w+)', title, re.IGNORECASE)
        if vs_match:
            team1 = vs_match.group(1).lower()
            team2 = vs_match.group(2).lower()
            if team1 not in self.STOP_WORDS and team2 not in self.STOP_WORDS:
                # Sort to ensure "A vs B" matches "B vs A"
                teams_sorted = '_'.join(sorted([team1, team2]))
                keys.append(f"vs_{teams_sorted}")
        
        return keys
    
    def _are_truly_related(self, title1: str, title2: str) -> bool:
        """Secondary validation - check if titles are actually about the same thing"""
        
        t1 = title1.lower()
        t2 = title2.lower()
        
        # Remove years for comparison
        t1 = self.YEAR_PATTERN.sub('', t1)
        t2 = self.YEAR_PATTERN.sub('', t2)
        
        # Extract significant words
        words1 = set(w for w in re.findall(r'\b\w+\b', t1) 
                     if w not in self.STOP_WORDS and len(w) > 3)
        words2 = set(w for w in re.findall(r'\b\w+\b', t2) 
                     if w not in self.STOP_WORDS and len(w) > 3)
        
        if not words1 or not words2:
            return False
        
        # Calculate Jaccard similarity
        intersection = len(words1 & words2)
        union = len(words1 | words2)
        
        similarity = intersection / union if union > 0 else 0
        
        # Require at least 40% word overlap
        return similarity >= 0.4