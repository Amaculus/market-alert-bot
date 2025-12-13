"""
Market Clustering Engine - Multiprocessing Optimized

Groups similar markets from different platforms (Kalshi/Polymarket)
to create a unified view of a topic and aggregate volume.
Uses ProcessPoolExecutor to bypass the Python GIL for heavy text comparisons.
"""

import os
import logging
from typing import List
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

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

# --- STANDALONE HELPER FUNCTIONS (Must be outside class for pickling) ---

def _normalize(text: str) -> str:
    """Clean text for comparison"""
    return text.lower().replace("will", "").replace("?", "").strip()

def _are_titles_similar(title1: str, title2: str, threshold: float) -> bool:
    """Check if two titles represent the same question"""
    t1 = _normalize(title1)
    t2 = _normalize(title2)
    
    # Fast path: substring match
    if t1 in t2 or t2 in t1:
        return True
        
    # Slow path: sequence matcher
    return SequenceMatcher(None, t1, t2).ratio() > threshold

def _process_chunk(markets: List[Market], threshold: float) -> List[MarketCluster]:
    """
    Process a single chunk of markets serially.
    This runs inside a worker process.
    """
    # Sort by volume desc
    sorted_markets = sorted(markets, key=lambda x: x.volume, reverse=True)
    local_clusters: List[MarketCluster] = []
    
    for market in sorted_markets:
        matched = False
        for cluster in local_clusters:
            if _are_titles_similar(market.title, cluster.primary_market.title, threshold):
                cluster.related_markets.append(market)
                matched = True
                break
        
        if not matched:
            local_clusters.append(MarketCluster(primary_market=market))
            
    return local_clusters


class ClusteringEngine:
    
    def __init__(self, similarity_threshold: float = 0.85):
        self.threshold = similarity_threshold
        # Determine strict parallelism limits to prevent freezing
        # Adaptive worker count based on available memory
        try:
            import psutil
            available_ram_gb = psutil.virtual_memory().available / (1024**3)
            cpu_count = multiprocessing.cpu_count()
            
            # Rule of thumb: 1 worker per 0.5GB available RAM, capped by CPUs
            max_workers_by_ram = int(available_ram_gb / 0.5)
            max_workers_by_cpu = min(8, max(1, cpu_count - 1))  # Cap at 16
            
            self.max_workers = min(max_workers_by_ram, max_workers_by_cpu)
            
            logger.info(f"Available RAM: {available_ram_gb:.1f}GB, CPUs: {cpu_count}")
            logger.info(f"ClusteringEngine using {self.max_workers} workers")
            
        except ImportError:
            # Fallback if psutil not available
            self.max_workers = 8
            logger.warning("psutil not available, defaulting to 8 workers")

    def cluster_markets(self, markets: List[Market]) -> List[MarketCluster]:
        """
        Group markets into clusters. 
        """
        if not markets:
            return []

        # Always use serial - parallel causes memory issues in containers
        return self._cluster_serial(markets)
    def _cluster_serial(self, markets: List[Market]) -> List[MarketCluster]:
        """Standard serial clustering"""
        return _process_chunk(markets, self.threshold)

    def _cluster_parallel(self, markets: List[Market]) -> List[MarketCluster]:
        """
        Map-Reduce style clustering:
        1. Split markets into N chunks
        2. Cluster each chunk in parallel (Map)
        3. Merge the resulting clusters (Reduce)
        """
        logger.info(f"Starting parallel clustering on {len(markets)} markets with {self.max_workers} cores...")
        
        # 1. Chunk the data
        chunk_size = max(1, len(markets) // self.max_workers)
        chunks = [markets[i:i + chunk_size] for i in range(0, len(markets), chunk_size)]
        
        results = []
        
        # 2. Parallel Processing
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(_process_chunk, chunk, self.threshold) for chunk in chunks]
            
            for future in as_completed(futures):
                try:
                    chunk_clusters = future.result()
                    results.extend(chunk_clusters)
                except Exception as e:
                    logger.error(f"Cluster worker failed: {e}")

        logger.info(f"Parallel phase done. Merging {len(results)} intermediate clusters...")
        
        # 3. Merge Phase (Serial)
        # We take the intermediate clusters and cluster them again.
        # This is fast because we are clustering "Clusters" not "Markets"
        
        final_clusters: List[MarketCluster] = []
        
        # Sort intermediate clusters by their TOTAL volume to prioritize big topics
        sorted_results = sorted(results, key=lambda c: c.total_volume, reverse=True)
        
        for candidate in sorted_results:
            matched = False
            for existing in final_clusters:
                # Compare the "Primary Market" titles of the clusters
                if _are_titles_similar(candidate.primary_market.title, existing.primary_market.title, self.threshold):
                    # Merge content: Candidate's primary + related go into Existing's related
                    existing.related_markets.append(candidate.primary_market)
                    existing.related_markets.extend(candidate.related_markets)
                    matched = True
                    break
            
            if not matched:
                final_clusters.append(candidate)
                
        return final_clusters