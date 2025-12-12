"""
AI-Powered Topic Relevance Checker

Uses OpenAI API to determine if a prediction market topic is relevant
for sports/entertainment betting content sites.
"""

import os
import re
from typing import Dict
import logging
from openai import OpenAI
from models import TopicCache
logger = logging.getLogger(__name__)


class RelevanceChecker:
    """Checks if market topics are relevant using AI"""
    
    def __init__(self):
        """Initialize the relevance checker"""
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.warning("OPENAI_API_KEY not set - using fallback relevance checking")
            self.client = None
        else:
            self.client = OpenAI(api_key=api_key)
        
        # Known high-value topics (Tier S) - always relevant
        self.tier_s_keywords = {
            # Sports
            'nfl', 'nba', 'mlb', 'nhl', 'lebron', 'mahomes', 'curry', 'world cup',
            'super bowl', 'playoffs', 'championship', 'finals',
            
            # Entertainment
            'taylor swift', 'drake', 'beyonce', 'marvel', 'star wars', 'disney',
            'netflix', 'hbo', 'game of thrones',
            
            # Gaming
            'gta 6', 'grand theft auto', 'call of duty', 'fortnite', 'minecraft',
            'playstation', 'xbox', 'nintendo',
            
            # Awards
            'oscars', 'grammys', 'emmys', 'academy award', 'golden globe',
            
            # Politics (for election markets)
            'election', 'president', 'senate', 'congress'
        }
        
        # Known low-value topics - never relevant
        self.irrelevant_keywords = {
            'weather', 'temperature', 'snowfall', 'cryptocurrency', 'bitcoin',
            'local', 'county', 'municipal'
        }
    
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
        # --- OPTIMIZATION START ---
        # 1. Check Database Cache
        # (Ensure you import TopicCache from models at the top of the file)
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
            # Log but don't crash if cache fails (e.g. table doesn't exist yet)
            logger.warning(f"Cache lookup failed: {e}")
        # --- OPTIMIZATION END ---
        
        # Extract main topic from title
        topic = self._extract_topic(market_title)
        
        # Quick check against known lists
        title_lower = market_title.lower()
        
        # Helper to cache results
        def cache_and_return(result):
            try:
                from models import TopicCache
                TopicCache.set(market_title, result)
            except Exception as e:
                logger.warning(f"Failed to cache result: {e}")
            return result
        
        # Check Tier S keywords
        for keyword in self.tier_s_keywords:
            if keyword in title_lower:
                return cache_and_return({
                    'is_relevant': True,
                    'tier': 'S',
                    'reasoning': f'High-value topic: {keyword}',
                    'topic': topic
                })
        
        # Check irrelevant keywords
        for keyword in self.irrelevant_keywords:
            if keyword in title_lower:
                return cache_and_return({
                    'is_relevant': False,
                    'tier': 'C',
                    'reasoning': f'Low-value topic: {keyword}',
                    'topic': topic
                })
        
        # Use AI for everything else
        if self.client:
            result = self._check_with_ai(market_title, topic)
            return cache_and_return(result)
        else:
            # Fallback: moderate tier
            return {
                'is_relevant': True,
                'tier': 'A',
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

Context: This is for Vegas Insider and Action Network - sports/entertainment betting news sites that cover:
- Major sports (NFL, NBA, MLB, etc.)
- Entertainment (movies, music, TV, celebrities)
- Gaming (major video game releases)
- Pop culture events (awards shows, major releases)

Determine:
1. Is this topic relevant for these sites? (YES or NO)
2. What tier is it?
   - S = A-list topic with high search demand (LeBron, Taylor Swift, Oscars, GTA 6)
   - A = Popular but not top-tier (B-list celebrities, indie games, rising stars)
   - C = Niche or unknown (local events, obscure topics, low search potential)

3. Brief reason (one sentence)

Format your response EXACTLY as:
RELEVANT: YES/NO
TIER: S/A/C
REASON: [one sentence]"""

            response = self.client.chat.completions.create(
                model="gpt-4o-mini",  # Fast and cheap model
                messages=[
                    {
                        "role": "system",
                        "content": "You are a content strategist analyzing prediction market topics for sports and entertainment betting sites."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=150,
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
            # Fallback to moderate relevance on error
            return {
                'is_relevant': True,
                'tier': 'A',
                'reasoning': 'AI check failed - defaulting to moderate relevance',
                'topic': topic
            }


# Example usage
if __name__ == "__main__":
    checker = RelevanceChecker()
    
    test_markets = [
        "Will Taylor Swift win Grammy Album of the Year?",
        "Will GTA 6 release in 2025?",
        "NYC snowfall over 30 inches this winter?",
        "Will LeBron James retire this season?",
        "Bitcoin price above $150K by end of year?",
    ]
    
    print("Testing Relevance Checker (OpenAI)")
    print("=" * 60)
    
    for market in test_markets:
        result = checker.check_relevance(market)
        print(f"\nMarket: {market}")
        print(f"  Relevant: {result['is_relevant']}")
        print(f"  Tier: {result['tier']}")
        print(f"  Reason: {result['reasoning']}")