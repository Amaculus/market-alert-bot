"""
AI-Powered Topic Relevance Checker
Optimized for Sports/Betting (ActionNetwork/VegasInsider focus)
Includes:
1. Aggressive Noise Filtering (Blacklist)
2. Content Prioritization (Whitelist)
3. Parallel Processing with Rate Limiting (ThreadPool)
"""

import os
import re
import logging
import time
import threading
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI
from models import TopicCache

logger = logging.getLogger(__name__)

class RelevanceChecker:
    # === CONFIGURATION ===
    MAX_WORKERS = 5            # Number of parallel AI threads
    MAX_RPM = 50               # Max OpenAI requests per minute (Safety buffer)
    
    # === 1. BLACKLIST (Immediate Fail) ===
    BLACKLIST_KEYWORDS = [
        # Weather & Science (High noise on prediction markets)
        r'temperature', r'degrees?', r'fahrenheit', r'celsius', 
        r'rain(fall)?', r'snow(fall)?', r'precipitation', r'weather',
        r'heat index', r'wind speed', r'NOAA', r'NASA', r'hurricane',
        
        # Niche Finance (Irrelevant for sports/pop-culture)
        r'closing price', r'market cap', r'fed funds', r'treasury', 
        r'mortgage rate', r'gas price', r'brent crude', r'WTI',
        r'eur/usd', r'yen', r'forex', r'commodity', 
        r'jobless claims', r'cpi', r'ppi', r'inflation',
        
        # Spam / Recurring / Low Value
        r'TSA check(point)?s?', r'covid', r'pandemic', 
        r'spotify', r'billboard', r'metacritic', r'rotten tomatoes',
        r'box office',
        
        # Local/Bureaucratic Politics
        r'mayor of', r'city council', r'comptroller', r'local election',
        r'transit', r'subway', r'approval rating'
    ]

    # === 2. WHITELIST (Immediate Pass) ===
    RELEVANCE_CATEGORIES = {
        # --- TIER S: MAJOR SPORTS & BETTING ---
        'SPORTS_MAJOR': {
            'tier': 'S',
            'keywords': [
                r'\bNFL\b', r'\bNBA\b', r'\bMLB\b', r'\bNHL\b', r'\bNCAAF\b', r'\bNCAAB\b',
                r'\bUFC\b', r'Super Bowl', r'World Series', r'Stanley Cup', 
                r'March Madness', r'Playoff', r'Championship', r'Premier League'
            ]
        },
        'SPORTS_BETTING': {
            'tier': 'S',
            'keywords': [
                r'moneyline', r'spread', r'over/under', r'parlay', r'prop bet',
                r'draft pick', r'MVP', r'Heisman', r'rookie of the year',
                r'touchdown', r'passing yards', r'rushing yards'
            ]
        },
        
        # --- TIER A: HIGH INTEREST ---
        'POP_CULTURE': {
            'tier': 'A',
            'keywords': [
                r'Taylor Swift', r'Drake', r'Oscar', r'Grammy',
                r'GTA', r'GTA6', r'Grand Theft Auto', r'Crypto', r'Bitcoin', r'BTC'
            ]
        },
        'POLITICS_US': {
            'tier': 'A',
            'keywords': [
                r'President', r'White House', r'Senate', r'Congress', 
                r'Election', r'Democrat', r'Republican', r'Trump', r'Harris'
            ]
        }
    }

    def __init__(self):
        # OpenAI Setup
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.warning("OPENAI_API_KEY not set - AI features disabled")
            self.client = None
        else:
            self.client = OpenAI(api_key=api_key)
        
        # Rate Limiting State
        self._rate_lock = threading.Lock()
        self._minute_start = time.time()
        self._request_count = 0

    def check_relevance_batch(self, titles: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Process a batch of titles in parallel.
        1. Checks Cache/Blacklist/Whitelist (Fast, Main Thread)
        2. Sends remaining to OpenAI (Parallel, Rate Limited)
        """
        results = {}
        to_process_ai = []

        # Step 1: Fast Static Checks
        for title in titles:
            static_result = self._check_static_rules(title)
            if static_result:
                results[title] = static_result
            else:
                to_process_ai.append(title)

        # Step 2: Parallel AI Checks
        if to_process_ai and self.client:
            logger.info(f"Checking {len(to_process_ai)} markets with AI (Parallel)...")
            
            with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                # Map future to title
                future_to_title = {
                    executor.submit(self._check_with_ai_throttled, title): title 
                    for title in to_process_ai
                }
                
                for future in as_completed(future_to_title):
                    title = future_to_title[future]
                    try:
                        results[title] = future.result()
                    except Exception as e:
                        logger.error(f"Error processing '{title}': {e}")
                        # Fallback if AI crashes
                        results[title] = {
                            'is_relevant': False, 
                            'tier': 'C', 
                            'reasoning': 'AI Error',
                            'topic': self._extract_topic(title)
                        }
        
        # Fill in any missing (e.g., if no AI client)
        for title in to_process_ai:
            if title not in results:
                results[title] = {
                    'is_relevant': False, 
                    'tier': 'C', 
                    'reasoning': 'No AI Client',
                    'topic': self._extract_topic(title)
                }

        return results

    def _check_static_rules(self, title: str) -> Optional[Dict]:
        """Checks DB Cache, Blacklist, and Whitelist. Returns None if AI is needed."""
        
        # 1. DB Cache
        try:
            cached = TopicCache.get(title)
            if cached:
                return {
                    'is_relevant': cached.is_relevant,
                    'tier': cached.tier,
                    'reasoning': f"[CACHED] {cached.reasoning}",
                    'topic': self._extract_topic(title)
                }
        except Exception:
            pass

        title_lower = title.lower()
        topic = self._extract_topic(title)

        # 2. Blacklist (Hard Pass)
        for pattern in self.BLACKLIST_KEYWORDS:
            if re.search(pattern, title_lower):
                return self._cache_and_return(title, {
                    'is_relevant': False,
                    'tier': 'C',
                    'reasoning': f"[BLACKLIST] Matched '{pattern}'",
                    'topic': topic
                })

        # 3. Whitelist (Fast Pass)
        for category, data in self.RELEVANCE_CATEGORIES.items():
            for pattern in data['keywords']:
                if re.search(pattern, title_lower, re.IGNORECASE):
                    return self._cache_and_return(title, {
                        'is_relevant': True,
                        'tier': data['tier'],
                        'reasoning': f"[WHITELIST] Matched {category} keyword '{pattern}'",
                        'topic': topic
                    })
        
        return None

    def _check_with_ai_throttled(self, title: str) -> Dict:
        """Wrapper for AI call that enforces rate limits."""
        self._wait_for_rate_limit()
        return self._check_with_ai_logic(title)

    def _wait_for_rate_limit(self):
        """Thread-safe rate limiter."""
        with self._rate_lock:
            now = time.time()
            # Reset counter if a minute has passed
            if now - self._minute_start >= 60:
                self._minute_start = now
                self._request_count = 0
            
            # Sleep if limit reached
            if self._request_count >= self.MAX_RPM:
                sleep_time = 60 - (now - self._minute_start) + 1
                logger.info(f"Rate limit hit ({self.MAX_RPM}/min). Sleeping {sleep_time:.1f}s...")
                time.sleep(sleep_time)
                self._minute_start = time.time()
                self._request_count = 0
            
            self._request_count += 1

    def _check_with_ai_logic(self, market_title: str) -> Dict:
        """Actual OpenAI API call."""
        topic = self._extract_topic(market_title)
        try:
            prompt = f"""Is this prediction market relevant for a Sports Betting & Pop Culture news site (ActionNetwork/Barstool)?

Market: "{market_title}"

Rules:
- YES: Sports, Betting, Major US Politics, Top Celebs, Big Tech (GTA/Crypto).
- NO: Weather, obscure finance, local politics, science.

Reply strictly:
RELEVANT: YES/NO
TIER: S (Major Sport), A (High Interest), B (Standard)
REASON: Brief phrase"""

            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=60,
                temperature=0.0
            )
            
            text = response.choices[0].message.content
            is_relevant = 'YES' in text.split('\n')[0].upper()
            
            tier_match = re.search(r'TIER:\s*([SAB])', text, re.IGNORECASE)
            tier = tier_match.group(1).upper() if tier_match else 'B'
            
            reason_match = re.search(r'REASON:\s*(.+)', text, re.IGNORECASE)
            reasoning = reason_match.group(1).strip() if reason_match else 'AI analysis'
            
            return self._cache_and_return(market_title, {
                'is_relevant': is_relevant,
                'tier': tier,
                'reasoning': f'[AI] {reasoning}',
                'topic': topic
            })
            
        except Exception as e:
            logger.error(f"AI Call Failed: {e}")
            return {
                'is_relevant': True, # Fail open (safe)
                'tier': 'B',
                'reasoning': '[AI ERROR]',
                'topic': topic
            }

    def _extract_topic(self, title: str) -> str:
        clean = re.sub(r'will\s+', '', title, flags=re.IGNORECASE)
        clean = re.sub(r'\?', '', clean)
        return ' '.join(clean.strip().split()[:4])

    def _cache_and_return(self, title: str, result: Dict) -> Dict:
        try:
            TopicCache.set(title, result)
        except Exception:
            pass
        return result