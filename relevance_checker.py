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

    # === STATIC RULES - Expanded to catch more patterns ===
    STATIC_RELEVANCE_RULES = {
        # === POLITICS (Tier S) ===
        'trump': ('S', 'US Politics'),
        'donald trump': ('S', 'US Politics'),
        'vance': ('S', 'US Politics'),
        'jd vance': ('S', 'US Politics'),
        'kamala': ('S', 'US Politics'),
        'harris': ('S', 'US Politics'),
        'biden': ('S', 'US Politics'),
        'desantis': ('A', 'US Politics'),
        'newsom': ('A', 'US Politics'),
        'rfk': ('A', 'US Politics'),
        'kennedy': ('A', 'US Politics'),
        'election': ('S', 'Politics'),
        'president': ('S', 'Politics'),
        'senate': ('S', 'Politics'),
        'congress': ('A', 'Politics'),
        'republican': ('A', 'Politics'),
        'democrat': ('A', 'Politics'),
        'gop': ('A', 'Politics'),
        'electoral': ('S', 'Politics'),
        'swing state': ('A', 'Politics'),
        'cabinet': ('A', 'Politics'),
        'secretary': ('A', 'Politics'),
        'nominee': ('A', 'Politics'),
        'supreme court': ('S', 'Politics'),
        'scotus': ('S', 'Politics'),

        # === SPORTS: NFL (Tier S/A) ===
        'nfl': ('S', 'League'),
        'super bowl': ('S', 'Major Event'),
        'superbowl': ('S', 'Major Event'),
        'touchdown': ('A', 'NFL'),
        'quarterback': ('A', 'NFL'),
        'chiefs': ('S', 'NFL Team'),
        'eagles': ('A', 'NFL Team'),
        'cowboys': ('A', 'NFL Team'),
        '49ers': ('A', 'NFL Team'),
        'ravens': ('A', 'NFL Team'),
        'bills': ('A', 'NFL Team'),
        'lions': ('A', 'NFL Team'),
        'mahomes': ('S', 'Star Player'),
        'kelce': ('S', 'Star Player'),
        'travis kelce': ('S', 'Star Player'),
        'lamar jackson': ('A', 'Star Player'),
        'josh allen': ('A', 'Star Player'),
        'burrow': ('A', 'Star Player'),
        'rodgers': ('A', 'Star Player'),
        'dak prescott': ('A', 'Star Player'),
        'cj stroud': ('A', 'Star Player'),
        'hurts': ('A', 'Star Player'),
        'purdy': ('A', 'Star Player'),
        'mvp': ('A', 'Award'),

        # === SPORTS: NBA (Tier S/A) ===
        'nba': ('S', 'League'),
        'finals': ('S', 'Major Event'),
        'playoffs': ('A', 'Event'),
        'lakers': ('S', 'NBA Team'),
        'celtics': ('A', 'NBA Team'),
        'warriors': ('A', 'NBA Team'),
        'heat': ('A', 'NBA Team'),
        'knicks': ('A', 'NBA Team'),
        'lebron': ('S', 'Star Player'),
        'lebron james': ('S', 'Star Player'),
        'curry': ('S', 'Star Player'),
        'steph curry': ('S', 'Star Player'),
        'stephen curry': ('S', 'Star Player'),
        'durant': ('A', 'Star Player'),
        'kevin durant': ('A', 'Star Player'),
        'giannis': ('A', 'Star Player'),
        'antetokounmpo': ('A', 'Star Player'),
        'jokic': ('A', 'Star Player'),
        'nikola jokic': ('A', 'Star Player'),
        'luka': ('A', 'Star Player'),
        'doncic': ('A', 'Star Player'),
        'tatum': ('A', 'Star Player'),
        'wembanyama': ('A', 'Star Player'),
        'wemby': ('A', 'Star Player'),
        'victor wembanyama': ('A', 'Star Player'),
        'anthony edwards': ('A', 'Star Player'),

        # === SPORTS: MLB (Tier A/S) ===
        'mlb': ('S', 'League'),
        'world series': ('S', 'Major Event'),
        'yankees': ('A', 'MLB Team'),
        'dodgers': ('S', 'MLB Team'),
        'mets': ('A', 'MLB Team'),
        'ohtani': ('S', 'Global Star'),
        'shohei': ('S', 'Global Star'),
        'shohei ohtani': ('S', 'Global Star'),
        'aaron judge': ('A', 'Star Player'),
        'judge': ('A', 'Star Player'),
        'harper': ('A', 'Star Player'),
        'bryce harper': ('A', 'Star Player'),
        'soto': ('A', 'Star Player'),
        'juan soto': ('A', 'Star Player'),
        'mookie': ('A', 'Star Player'),
        'betts': ('A', 'Star Player'),
        'home run': ('A', 'MLB'),
        'cy young': ('A', 'Award'),

        # === SPORTS: SOCCER (Tier A/S) ===
        'soccer': ('A', 'Sport'),
        'football': ('A', 'Sport'),  # Could be NFL or soccer
        'premier league': ('A', 'League'),
        'champions league': ('A', 'Major Event'),
        'world cup': ('S', 'Global Event'),
        'la liga': ('A', 'League'),
        'serie a': ('A', 'League'),
        'bundesliga': ('A', 'League'),
        'messi': ('S', 'Global Star'),
        'lionel messi': ('S', 'Global Star'),
        'ronaldo': ('S', 'Global Star'),
        'cristiano': ('S', 'Global Star'),
        'mbappe': ('S', 'Star Player'),
        'kylian': ('S', 'Star Player'),
        'haaland': ('A', 'Star Player'),
        'erling': ('A', 'Star Player'),
        'bellingham': ('A', 'Star Player'),
        'jude bellingham': ('A', 'Star Player'),
        'yamal': ('A', 'Star Player'),
        'neymar': ('A', 'Star Player'),
        'manchester': ('A', 'Soccer'),
        'liverpool': ('A', 'Soccer'),
        'arsenal': ('A', 'Soccer'),
        'chelsea': ('A', 'Soccer'),
        'barcelona': ('A', 'Soccer'),
        'real madrid': ('A', 'Soccer'),

        # === SPORTS: OTHER (Tier A) ===
        'ufc': ('A', 'Combat Sports'),
        'mma': ('A', 'Combat Sports'),
        'f1': ('A', 'Motorsports'),
        'formula 1': ('A', 'Motorsports'),
        'formula one': ('A', 'Motorsports'),
        'nascar': ('A', 'Motorsports'),
        'golf': ('A', 'Sport'),
        'pga': ('A', 'Golf'),
        'masters': ('A', 'Golf'),
        'tennis': ('A', 'Sport'),
        'wimbledon': ('A', 'Tennis'),
        'us open': ('A', 'Tennis/Golf'),
        'mcgregor': ('A', 'Star Fighter'),
        'conor mcgregor': ('A', 'Star Fighter'),
        'jon jones': ('A', 'Star Fighter'),
        'verstappen': ('A', 'Star Driver'),
        'max verstappen': ('A', 'Star Driver'),
        'hamilton': ('A', 'Star Driver'),
        'lewis hamilton': ('A', 'Star Driver'),
        'tiger woods': ('A', 'Golf Legend'),
        'djokovic': ('A', 'Tennis Star'),
        'nadal': ('A', 'Tennis Star'),
        'federer': ('A', 'Tennis Star'),

        # === MUSIC & POP CULTURE (Tier S/A) ===
        'taylor swift': ('S', 'Megastar'),
        'swift': ('S', 'Megastar'),
        'taylor': ('A', 'Possible Megastar'),  # Context dependent
        'beyonce': ('S', 'Megastar'),
        'beyoncé': ('S', 'Megastar'),
        'drake': ('S', 'Megastar'),
        'kendrick': ('S', 'Megastar'),
        'kendrick lamar': ('S', 'Megastar'),
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
        'kardashian': ('A', 'Celeb'),
        'elon musk': ('S', 'Tech Celeb'),
        'elon': ('A', 'Tech Celeb'),
        'musk': ('A', 'Tech Celeb'),
        'zuckerberg': ('A', 'Tech Celeb'),
        'bezos': ('A', 'Tech Celeb'),
        'oscar': ('S', 'Awards'),
        'oscars': ('S', 'Awards'),
        'academy award': ('S', 'Awards'),
        'grammy': ('S', 'Awards'),
        'grammys': ('S', 'Awards'),
        'emmy': ('A', 'Awards'),
        'emmys': ('A', 'Awards'),
        'golden globe': ('A', 'Awards'),
        'super bowl halftime': ('S', 'Event'),
        'coachella': ('A', 'Event'),

        # === MOVIES/TV (Tier A) ===
        'marvel': ('A', 'Entertainment'),
        'disney': ('A', 'Entertainment'),
        'netflix': ('A', 'Entertainment'),
        'hbo': ('A', 'Entertainment'),
        'star wars': ('A', 'Entertainment'),
        'box office': ('A', 'Entertainment'),
        'streaming': ('B', 'Entertainment'),

        # === GAMING (Tier S/A) ===
        'gta': ('S', 'Major Game'),
        'gta 6': ('S', 'Major Game'),
        'gta vi': ('S', 'Major Game'),
        'grand theft auto': ('S', 'Major Game'),
        'call of duty': ('A', 'Major Game'),
        'cod': ('A', 'Major Game'),
        'fortnite': ('A', 'Major Game'),
        'minecraft': ('A', 'Major Game'),
        'nintendo': ('A', 'Gaming'),
        'playstation': ('A', 'Gaming'),
        'xbox': ('A', 'Gaming'),
        'switch': ('B', 'Gaming'),
        'esports': ('B', 'Gaming'),
        'twitch': ('B', 'Gaming'),

        # === TECH (Tier A/B) ===
        'apple': ('A', 'Tech'),
        'iphone': ('A', 'Tech'),
        'google': ('A', 'Tech'),
        'microsoft': ('A', 'Tech'),
        'tesla': ('A', 'Tech'),
        'openai': ('A', 'Tech'),
        'chatgpt': ('A', 'Tech'),
        'ai': ('B', 'Tech'),
        'artificial intelligence': ('B', 'Tech'),
        'spacex': ('A', 'Tech'),
        'twitter': ('A', 'Tech'),
        'x.com': ('A', 'Tech'),
        'tiktok': ('A', 'Tech'),

        # === ECONOMY (Tier A/B) ===
        'fed': ('A', 'Economy'),
        'federal reserve': ('A', 'Economy'),
        'fed rates': ('B', 'Economy'),
        'interest rate': ('B', 'Economy'),
        'rate cut': ('B', 'Economy'),
        'rate hike': ('B', 'Economy'),
        'cpi': ('B', 'Economy'),
        'inflation': ('B', 'Economy'),
        'recession': ('B', 'Economy'),
        'stock market': ('B', 'Economy'),
        's&p': ('B', 'Economy'),
        'dow jones': ('B', 'Economy'),
        'nasdaq': ('B', 'Economy'),

        # === CRYPTO (Tier A/B) ===
        'bitcoin': ('A', 'Crypto'),
        'btc': ('A', 'Crypto'),
        'ethereum': ('A', 'Crypto'),
        'eth': ('A', 'Crypto'),
        'crypto': ('B', 'Crypto'),
        'solana': ('B', 'Crypto'),
        'dogecoin': ('B', 'Crypto'),

        # === COUNTRIES (Tier B) ===
        'usa': ('B', 'Major Economy'),
        'united states': ('B', 'Major Economy'),
        'china': ('B', 'Major Economy'),
        'russia': ('B', 'Geopolitics'),
        'ukraine': ('B', 'Geopolitics'),
        'israel': ('B', 'Geopolitics'),
        'iran': ('B', 'Geopolitics'),
        'uk': ('B', 'Major Economy'),
        'united kingdom': ('B', 'Major Economy'),
        'japan': ('B', 'Major Economy'),
        'germany': ('B', 'Major Economy'),
        'france': ('B', 'Major Economy'),
        'canada': ('B', 'Major Economy'),
        'india': ('B', 'Major Economy'),
        'brazil': ('B', 'Major Economy'),
    }

    IRRELEVANT_KEYWORDS = {
        'weather', 'temperature', 'snowfall', 'rainfall', 'rain', 'snow',
        'humidity', 'celsius', 'fahrenheit',
        'local', 'municipal', 'county', 'sheriff', 'mayor', 'city council',
        'subway', 'transit', 'bus route', 'measure', 'prop',
        'daily active users', 'friend.tech', 'manifold', 'metaculus',
        'polymarket volume', 'kalshi volume',
        # Niche/obscure
        'obscure', 'local election',
    }

    def __init__(self):
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.warning("OPENAI_API_KEY not set - using fallback relevance checking")
            self.client = None
        else:
            self.client = OpenAI(api_key=api_key)
        
        # Track AI calls for debugging
        self.ai_call_count = 0
    
    def check_relevance(self, market_title: str) -> Dict:
        """Check if a market topic is relevant"""
        
        # 1. Check Database Cache
        try:
            cached = TopicCache.get(market_title)
            if cached:
                return {
                    'is_relevant': cached.is_relevant,
                    'tier': cached.tier,
                    'reasoning': f"[CACHED] {cached.reasoning}",
                    'topic': self._extract_topic(market_title)
                }
        except Exception:
            pass
        
        topic = self._extract_topic(market_title)
        title_lower = market_title.lower()
        
        def cache_and_return(result):
            try:
                TopicCache.set(market_title, result)
            except Exception:
                pass
            return result
        
        # 2. Check Irrelevant Keywords (Fast Fail)
        for keyword in self.IRRELEVANT_KEYWORDS:
            if keyword in title_lower:
                result = {
                    'is_relevant': False,
                    'tier': 'C',
                    'reasoning': f'[RULE:IRRELEVANT] matched: {keyword}',
                    'topic': topic
                }
                logger.debug(f"SKIP (irrelevant rule): {market_title[:60]}...")
                return cache_and_return(result)

        # 3. Check Static Relevant Rules (Fast Pass)
        for keyword, (tier, reason) in self.STATIC_RELEVANCE_RULES.items():
            if keyword in title_lower:
                result = {
                    'is_relevant': True,
                    'tier': tier,
                    'reasoning': f'[RULE] {reason} ({keyword})',
                    'topic': topic
                }
                logger.debug(f"MATCH (rule): {market_title[:60]}... -> Tier {tier}")
                return cache_and_return(result)
        
        # 4. Use AI for everything else
        if self.client:
            self.ai_call_count += 1
            logger.info(f"[AI CALL #{self.ai_call_count}] No rule match for: {market_title[:80]}...")
            result = self._check_with_ai(market_title, topic)
            logger.info(f"[AI RESULT] relevant={result['is_relevant']}, tier={result['tier']}, reason={result['reasoning']}")
            return cache_and_return(result)
        else:
            # Fallback if no AI
            return {
                'is_relevant': True,
                'tier': 'B',
                'reasoning': '[FALLBACK] AI unavailable',
                'topic': topic
            }
    
    def _extract_topic(self, title: str) -> str:
        """Extract main topic/entity from market title"""
        title = re.sub(r'will\s+', '', title, flags=re.IGNORECASE)
        title = re.sub(r'\?', '', title)
        title = re.sub(r'prediction|market|odds|betting', '', title, flags=re.IGNORECASE)
        words = title.strip().split()[:4]
        return ' '.join(words)
    
    def _check_with_ai(self, market_title: str, topic: str) -> Dict:
        """Use OpenAI API to check relevance"""
        
        try:
            prompt = f"""Analyze this prediction market for content relevance:

Market: "{market_title}"

Context: This is for sports/entertainment betting news sites like Vegas Insider and Action Network.

Determine:
1. Is this topic relevant? (YES or NO)
2. Tier: S (A-list), A (popular), or C (niche)
3. One sentence reason

Format exactly:
RELEVANT: YES/NO
TIER: S/A/C
REASON: [reason]"""

            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a content strategist. Respond in the exact format requested."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=80,
                temperature=0.2
            )
            
            response_text = response.choices[0].message.content
            
            is_relevant = 'YES' in response_text.split('\n')[0].upper()
            
            tier_match = re.search(r'TIER:\s*([SAC])', response_text, re.IGNORECASE)
            tier = tier_match.group(1).upper() if tier_match else 'B'
            
            reason_match = re.search(r'REASON:\s*(.+)', response_text, re.IGNORECASE)
            reasoning = reason_match.group(1).strip() if reason_match else 'AI analysis'
            
            return {
                'is_relevant': is_relevant,
                'tier': tier,
                'reasoning': f'[AI] {reasoning}',
                'topic': topic
            }
            
        except Exception as e:
            logger.error(f"AI relevance check error: {e}")
            return {
                'is_relevant': True,
                'tier': 'B',
                'reasoning': '[AI ERROR] defaulting to moderate',
                'topic': topic
            }
