"""
AI-Powered Topic Relevance Checker with Static Rules Optimization
"""

import os
import re
from typing import Dict
import logging
from openai import OpenAI
from models import TopicCache

logger = logging.getLogger(__name__)

class RelevanceChecker:
    """Checks if market topics are relevant using Regex first, then AI"""

    # --- STATIC RULES CONFIGURATION ---
    # format: 'keyword': ('Tier', 'Reason')
    STATIC_RELEVANCE_RULES = {
        # === POLITICS (Tier S) ===
        'trump': ('S', 'US Politics'),
        'donald trump': ('S', 'US Politics'),
        'vance': ('S', 'US Politics'),
        'jd vance': ('S', 'US Politics'),
        'kamala': ('S', 'US Politics'),
        'harris': ('S', 'US Politics'),
        'election': ('S', 'Politics'),
        'president': ('S', 'Politics'),
        'senate': ('S', 'Politics'),

        # === SPORTS: NFL (Tier S/A) ===
        'nfl': ('S', 'League'),
        'super bowl': ('S', 'Major Event'),
        'mahomes': ('S', 'Star Player'),
        'kelce': ('S', 'Star Player'),
        'lamar jackson': ('A', 'Star Player'),
        'josh allen': ('A', 'Star Player'),
        'burrow': ('A', 'Star Player'),
        'rodgers': ('A', 'Star Player'),
        'dak prescott': ('A', 'Star Player'),
        'cj stroud': ('A', 'Star Player'),
        'hurts': ('A', 'Star Player'),
        'purdy': ('A', 'Star Player'),

        # === SPORTS: NBA (Tier S/A) ===
        'nba': ('S', 'League'),
        'finals': ('S', 'Major Event'),
        'lebron': ('S', 'Star Player'),
        'james': ('S', 'Star Player'),  # Context dependent, but usually LeBron in this feed
        'curry': ('S', 'Star Player'),
        'steph': ('S', 'Star Player'),
        'durant': ('A', 'Star Player'),
        'giannis': ('A', 'Star Player'),
        'jokic': ('A', 'Star Player'),
        'luka': ('A', 'Star Player'),
        'doncic': ('A', 'Star Player'),
        'tatum': ('A', 'Star Player'),
        'wembanyama': ('A', 'Star Player'),
        'wemby': ('A', 'Star Player'),
        'edwards': ('A', 'Star Player'), # Anthony Edwards

        # === SPORTS: MLB (Tier A/S) ===
        'mlb': ('S', 'League'),
        'world series': ('S', 'Major Event'),
        'ohtani': ('S', 'Global Star'),
        'shohei': ('S', 'Global Star'),
        'judge': ('A', 'Star Player'),
        'harper': ('A', 'Star Player'),
        'soto': ('A', 'Star Player'),
        'mookie': ('A', 'Star Player'),
        'betts': ('A', 'Star Player'),

        # === SPORTS: SOCCER (Tier A/S) ===
        'soccer': ('A', 'Sport'),
        'premier league': ('A', 'League'),
        'champions league': ('A', 'Major Event'),
        'world cup': ('S', 'Global Event'),
        'messi': ('S', 'Global Star'),
        'ronaldo': ('S', 'Global Star'),
        'mbappe': ('S', 'Star Player'),
        'haaland': ('A', 'Star Player'),
        'bellingham': ('A', 'Star Player'),
        'yamal': ('A', 'Star Player'),
        'neymar': ('A', 'Star Player'),

        # === SPORTS: OTHER (Tier A) ===
        'ufc': ('A', 'Combat Sports'),
        'f1': ('A', 'Motorsports'),
        'formula 1': ('A', 'Motorsports'),
        'mcgregor': ('A', 'Star Fighter'),
        'jon jones': ('A', 'Star Fighter'),
        'verstappen': ('A', 'Star Driver'),
        'hamilton': ('A', 'Star Driver'),

        # === MUSIC & POP CULTURE (Tier S/A) ===
        'taylor swift': ('S', 'Megastar'),
        'swift': ('S', 'Megastar'),
        'beyonce': ('S', 'Megastar'),
        'drake': ('S', 'Megastar'),
        'kendrick': ('S', 'Megastar'),
        'k-dot': ('S', 'Megastar'),
        'billie eilish': ('A', 'Pop Star'),
        'the weeknd': ('A', 'Pop Star'),
        'bad bunny': ('A', 'Global Star'),
        'dua lipa': ('A', 'Pop Star'),
        'harry styles': ('A', 'Pop Star'),
        'adele': ('A', 'Pop Star'),
        'lady gaga': ('A', 'Pop Star'),
        'rihanna': ('A', 'Pop Star'),
        'justin bieber': ('A', 'Pop Star'),
        'travis scott': ('A', 'Rap Star'),
        'kanye': ('A', 'Celeb'),
        'ye': ('A', 'Celeb'),
        'kim kardashian': ('A', 'Celeb'),
        'oscar': ('S', 'Awards'),
        'grammy': ('S', 'Awards'),
        'emmy': ('A', 'Awards'),

        # === GAMING (Tier S/A) ===
        'gta': ('S', 'Major Game'),
        'grand theft auto': ('S', 'Major Game'),
        'call of duty': ('A', 'Major Game'),
        'cod': ('A', 'Major Game'),
        'fortnite': ('A', 'Major Game'),
        'nintendo': ('A', 'Gaming'),
        'playstation': ('A', 'Gaming'),
        'xbox': ('A', 'Gaming'),

        # === COUNTRIES / ECONOMY (Tier A/B) ===
        # Focused on major economies/elections, avoiding war zones
        'usa': ('A', 'Major Economy'),
        'united states': ('A', 'Major Economy'),
        'china': ('A', 'Major Economy'),
        'uk': ('A', 'Major Economy'),
        'united kingdom': ('A', 'Major Economy'),
        'japan': ('A', 'Major Economy'),
        'germany': ('B', 'Major Economy'),
        'france': ('B', 'Major Economy'),
        'canada': ('B', 'Major Economy'),
        'india': ('B', 'Major Economy'),
        'brazil': ('B', 'Major Economy'),
        'fed rates': ('B', 'Economy'),
        'interest rates': ('B', 'Economy'),
        'cpi': ('B', 'Economy'),
        'inflation': ('B', 'Economy'),
        'bitcoin': ('A', 'Crypto'),
        'ethereum': ('A', 'Crypto'),
    }

    IRRELEVANT_KEYWORDS = {
        'weather', 'temperature', 'snowfall', 'rainfall',
        'local', 'municipal', 'county', 'sheriff', 'mayor',
        'subway', 'transit', 'bus', 'measure', 'prop',
        'daily active users', 'friend.tech', 'manifold',
        'war', 'invasion', 'casualty', 'deaths' # Explicitly ignore war specifics if needed
    }

    def __init__(self):
        """Initialize the relevance checker"""
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.warning("OPENAI_API_KEY not set - using fallback relevance checking")
            self.client = None
        else:
            self.client = OpenAI(api_key=api_key)
    
    def check_relevance(self, market_title: str) -> Dict:
        """
        Check if a market topic is relevant for content
        
        Returns:
            {
                'is_relevant': bool,
                'tier': 'S' | 'A' | 'C',
                'reasoning': str,
                'topic': str
            }
        """
        # 1. Check Database Cache
        try:
            from models import TopicCache
            cached = TopicCache.get(market_title)
            if cached:
                return {
                    'is_relevant': cached.is_relevant,
                    'tier': cached.tier,
                    'reasoning': cached.reasoning,
                    'topic': self._extract_topic(market_title)
                }
        except Exception as e:
            # Log but don't crash if cache fails
            pass
        
        # Extract topic for return value
        topic = self._extract_topic(market_title)
        title_lower = market_title.lower()
        
        # Helper to cache results
        def cache_and_return(result):
            try:
                from models import TopicCache
                TopicCache.set(market_title, result)
            except Exception:
                pass
            return result
        
        # 2. Check Irrelevant Keywords (Fast Fail)
        for keyword in self.IRRELEVANT_KEYWORDS:
            if keyword in title_lower:
                return cache_and_return({
                    'is_relevant': False,
                    'tier': 'C',
                    'reasoning': f'Rule match: {keyword}',
                    'topic': topic
                })

        # 3. Check Static Relevant Rules (Fast Pass)
        # We look for the keyword as a substring
        for keyword, (tier, reason) in self.STATIC_RELEVANCE_RULES.items():
            if keyword in title_lower:
                return cache_and_return({
                    'is_relevant': True,
                    'tier': tier,
                    'reasoning': f'Rule match: {reason} ({keyword})',
                    'topic': topic
                })
        
        # 4. Use AI for everything else (Expensive)
        if self.client:
            result = self._check_with_ai(market_title, topic)
            return cache_and_return(result)
        else:
            # Fallback if no AI
            return {
                'is_relevant': True,
                'tier': 'B',
                'reasoning': 'AI check unavailable - default moderate relevance',
                'topic': topic
            }
    
    def _extract_topic(self, title: str) -> str:
        """Extract main topic/entity from market title"""
        # Remove common prediction market phrases
        title = re.sub(r'will\s+', '', title, flags=re.IGNORECASE)
        title = re.sub(r'\?', '', title)
        title = re.sub(r'prediction|market|odds|betting', '', title, flags=re.IGNORECASE)
        
        # Extract first few meaningful words
        words = title.strip().split()[:4]
        return ' '.join(words)
    
    def _check_with_ai(self, market_title: str, topic: str) -> Dict:
        """Use OpenAI API to check relevance"""
        
        try:
            prompt = f"""Analyze this prediction market for content relevance:

Market: "{market_title}"

Context: This is for Vegas Insider and Action Network - sports/entertainment betting news sites.

Determine:
1. Is this topic relevant for these sites? (YES or NO)
2. What tier is it?
   - S = A-list topic (LeBron, Taylor Swift, Oscars, US Election)
   - A = Popular but not top-tier (Star players, rising stars, major countries)
   - C = Niche or unknown

3. Brief reason (one sentence)

Format:
RELEVANT: YES/NO
TIER: S/A/C
REASON: [reason]"""

            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a content strategist."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=100,
                temperature=0.3
            )
            
            response_text = response.choices[0].message.content
            
            # Parse response
            is_relevant = 'YES' in response_text.split('\n')[0].upper()
            
            tier_match = re.search(r'TIER:\s*([SAC])', response_text, re.IGNORECASE)
            tier = tier_match.group(1).upper() if tier_match else 'A'
            
            reason_match = re.search(r'REASON:\s*(.+)', response_text, re.IGNORECASE)
            reasoning = reason_match.group(1).strip() if reason_match else 'AI analysis completed'
            
            return {
                'is_relevant': is_relevant,
                'tier': tier,
                'reasoning': reasoning,
                'topic': topic
            }
            
        except Exception as e:
            logger.error(f"Error in AI relevance check: {e}")
            return {
                'is_relevant': True,
                'tier': 'A',
                'reasoning': 'AI check failed - defaulting to moderate',
                'topic': topic
            }