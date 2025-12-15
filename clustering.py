"""
Market Clustering Engine - Event-First Hybrid Architecture

Phase 1: Group markets by platform event_id (hard grouping)
Phase 2: Cross-platform text similarity (soft grouping)
"""

import re
import logging
from typing import List, Dict, Set, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor

from market import Market

logger = logging.getLogger(__name__)


@dataclass
class MarketCluster:
    """A group of related markets representing the same event"""
    primary_market: Market
    related_markets: List[Market] = field(default_factory=list)
    event_id: str = ""
    event_title: str = ""
    platforms: Set[str] = field(default_factory=set)
    
    @property
    def total_volume(self) -> float:
        return self.primary_market.volume + sum(m.volume for m in self.related_markets)
    
    @property
    def all_markets(self) -> List[Market]:
        return [self.primary_market] + self.related_markets
    
    @property
    def market_count(self) -> int:
        return 1 + len(self.related_markets)
    
    @property
    def platform_spread(self) -> List[str]:
        platforms = {self.primary_market.platform}
        for m in self.related_markets:
            platforms.add(m.platform)
        return list(platforms)
    
    @property
    def title(self) -> str:
        return self.event_title or self.primary_market.title
    
    def get_top_markets(self, n: int = 3) -> List[Market]:
        """Get top N markets by volume"""
        all_mkts = self.all_markets
        return sorted(all_mkts, key=lambda m: m.volume, reverse=True)[:n]
    
    def get_representative_text(self) -> str:
        """Generate representative text for cross-platform matching"""
        texts = []
        if self.event_title:
            texts.append(self.event_title)
        texts.append(self.primary_market.title)
        if self.primary_market.subtitle:
            texts.append(self.primary_market.subtitle)
        return " ".join(texts)


class ClusteringEngine:
    """Event-First Hybrid Clustering Engine"""
    
    STOP_WORDS = {
        'will', 'be', 'is', 'are', 'was', 'were', 'been', 'being',
        'have', 'has', 'had', 'do', 'does', 'did', 'doing',
        'can', 'could', 'would', 'should', 'may', 'might', 'must',
        'get', 'got', 'make', 'made', 'go', 'going', 'gone',
        'win', 'wins', 'won', 'lose', 'lost', 'beat', 'defeat',
        'become', 'becomes', 'happen', 'happens',
        'the', 'a', 'an', 'this', 'that', 'these', 'those',
        'i', 'you', 'he', 'she', 'it', 'we', 'they',
        'my', 'your', 'his', 'her', 'its', 'our', 'their',
        'who', 'what', 'which', 'where', 'when', 'why', 'how',
        'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by',
        'from', 'up', 'down', 'out', 'off', 'over', 'under',
        'about', 'into', 'through', 'during', 'before', 'after',
        'above', 'below', 'between', 'against', 'until', 'unless',
        'and', 'or', 'but', 'if', 'then', 'than', 'so', 'as',
        'next', 'first', 'last', 'new', 'old', 'best', 'top',
        'most', 'more', 'any', 'all', 'some', 'other', 'each',
        'market', 'prediction', 'odds', 'bet', 'betting', 'price',
        'yes', 'no', 'winner', 'lead', 'leading',
        'january', 'february', 'march', 'april', 'may', 'june',
        'july', 'august', 'september', 'october', 'november', 'december',
        'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
        'today', 'tomorrow', 'yesterday', 'week', 'month', 'year',
        'end', 'start', 'begin', 'beginning', 'ending',
    }
    
    YEAR_PATTERN = re.compile(r'\b20\d{2}\b')
    
    def __init__(self):
        self.max_workers = 8
    
    def cluster_markets(self, markets: List[Market]) -> List[MarketCluster]:
        """
        Event-First Hybrid Clustering:
        Step A: Hard group by (platform, event_id)
        Step B: Generate representative text for each group
        Step C: Soft group across platforms using text similarity
        Step D: Merge cross-platform matches
        """
        if not markets:
            return []
        
        logger.info(f"Clustering {len(markets)} markets using Event-First approach...")
        
        # === STEP A: Hard Grouping by Platform Event ID ===
        event_groups: Dict[str, List[Market]] = {}
        
        for market in markets:
            key = market.event_id or f"{market.platform}_{market.id}"
            if key not in event_groups:
                event_groups[key] = []
            event_groups[key].append(market)
        
        logger.info(f"Step A: Created {len(event_groups)} event groups from platform IDs")
        
        # === STEP B: Create Initial Clusters with Representatives ===
        initial_clusters: List[MarketCluster] = []
        
        for event_id, group_markets in event_groups.items():
            # Sort by volume, highest first
            sorted_group = sorted(group_markets, key=lambda m: m.volume, reverse=True)
            primary = sorted_group[0]
            
            # Determine event title
            event_title = None
            for m in sorted_group:
                if m.event_title:
                    event_title = m.event_title
                    break
            
            cluster = MarketCluster(
                primary_market=primary,
                related_markets=sorted_group[1:],
                event_id=event_id,
                event_title=event_title or primary.title,
                platforms={primary.platform}
            )
            
            initial_clusters.append(cluster)
        
        logger.info(f"Step B: Created {len(initial_clusters)} initial clusters")
        
        # === STEP C: Cross-Platform Soft Grouping ===
        # Generate keys for each cluster for cross-platform matching
        cluster_keys: Dict[str, List[MarketCluster]] = {}
        
        for cluster in initial_clusters:
            keys = self._generate_cluster_keys(cluster)
            for key in keys:
                if key not in cluster_keys:
                    cluster_keys[key] = []
                cluster_keys[key].append(cluster)
        
        # === STEP D: Merge Cross-Platform Matches ===
        merged_clusters: List[MarketCluster] = []
        merged_ids: Set[str] = set()
        
        # Sort by total volume descending
        sorted_clusters = sorted(initial_clusters, key=lambda c: c.total_volume, reverse=True)
        
        for cluster in sorted_clusters:
            if cluster.event_id in merged_ids:
                continue
            
            # Find all clusters that share keys with this one
            matching_clusters = set()
            keys = self._generate_cluster_keys(cluster)
            
            for key in keys:
                for other in cluster_keys.get(key, []):
                    if other.event_id != cluster.event_id and other.event_id not in merged_ids:
                        # Validate with text similarity
                        if self._clusters_are_related(cluster, other):
                            matching_clusters.add(other.event_id)
            
            # Merge matching clusters
            if matching_clusters:
                # This cluster absorbs all matches
                for other in sorted_clusters:
                    if other.event_id in matching_clusters:
                        cluster.related_markets.extend(other.all_markets)
                        cluster.platforms.update(other.platforms)
                        merged_ids.add(other.event_id)
                
                # Re-sort related markets by volume
                cluster.related_markets = sorted(
                    cluster.related_markets, 
                    key=lambda m: m.volume, 
                    reverse=True
                )
            
            merged_ids.add(cluster.event_id)
            merged_clusters.append(cluster)
        
        # Final sort by volume
        merged_clusters = sorted(merged_clusters, key=lambda c: c.total_volume, reverse=True)
        
        logger.info(f"Step D: Merged into {len(merged_clusters)} final clusters")
        
        # Log some examples
        for i, cluster in enumerate(merged_clusters[:5]):
            platforms = ", ".join(cluster.platform_spread)
            logger.info(
                f"  Cluster {i+1}: '{cluster.title[:50]}' | "
                f"{cluster.market_count} markets | {platforms} | "
                f"Vol: ${cluster.total_volume:,.0f}"
            )
        
        return merged_clusters
    
    def _generate_cluster_keys(self, cluster: MarketCluster) -> List[str]:
        """Generate lookup keys for cross-platform matching"""
        text = cluster.get_representative_text()
        
        # Remove years
        text = self.YEAR_PATTERN.sub('', text)
        
        text_lower = text.lower()
        text_clean = re.sub(r'[^a-z0-9\s]', ' ', text_lower)
        
        words = [w for w in text_clean.split() if w not in self.STOP_WORDS and len(w) > 2]
        
        keys = []
        
        # Key 1: Proper nouns
        proper_nouns = re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b', text)
        proper_nouns = [p.lower().replace(' ', '_') for p in proper_nouns if len(p) > 3]
        proper_nouns = [p for p in proper_nouns if p not in self.STOP_WORDS]
        
        if proper_nouns:
            longest = max(proper_nouns, key=len)
            if len(longest) > 4:
                keys.append(longest)
            
            if len(proper_nouns) >= 2:
                combo = f"{proper_nouns[0]}_{proper_nouns[1]}"
                keys.append(combo)
        
        # Key 2: Significant words
        if not keys and len(words) >= 3:
            specific_words = [w for w in words if len(w) >= 4]
            if len(specific_words) >= 2:
                keys.append('_'.join(specific_words[:3]))
        
        # Key 3: Team vs team pattern
        vs_match = re.search(r'(\w+)\s+(?:vs\.?|versus|v\.?|@)\s+(\w+)', text, re.IGNORECASE)
        if vs_match:
            team1 = vs_match.group(1).lower()
            team2 = vs_match.group(2).lower()
            if team1 not in self.STOP_WORDS and team2 not in self.STOP_WORDS:
                teams_sorted = '_'.join(sorted([team1, team2]))
                keys.append(f"vs_{teams_sorted}")
        
        return keys
    
    def _clusters_are_related(self, c1: MarketCluster, c2: MarketCluster) -> bool:
        """Validate cross-platform match with text similarity"""
        
        # Don't merge same-platform clusters (they should already be grouped)
        if c1.primary_market.platform == c2.primary_market.platform:
            return False
        
        t1 = c1.get_representative_text().lower()
        t2 = c2.get_representative_text().lower()
        
        t1 = self.YEAR_PATTERN.sub('', t1)
        t2 = self.YEAR_PATTERN.sub('', t2)
        
        words1 = set(w for w in re.findall(r'\b\w+\b', t1) 
                     if w not in self.STOP_WORDS and len(w) > 3)
        words2 = set(w for w in re.findall(r'\b\w+\b', t2) 
                     if w not in self.STOP_WORDS and len(w) > 3)
        
        if not words1 or not words2:
            return False
        
        intersection = len(words1 & words2)
        union = len(words1 | words2)
        
        similarity = intersection / union if union > 0 else 0
        
        # Require 35% overlap for cross-platform (slightly lower threshold)
        return similarity >= 0.35