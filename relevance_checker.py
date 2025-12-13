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

    # === STATIC RULES - Expanded ===
    STATIC_RELEVANCE_RULES = {
        # === POLITICS: US (Tier S) ===
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

        # === POLITICS: World Leaders (Tier A/B) ===
        'macron': ('A', 'World Politics'),
        'xi jinping': ('A', 'World Politics'),
        'putin': ('A', 'World Politics'),
        'zelensky': ('A', 'World Politics'),
        'netanyahu': ('A', 'World Politics'),
        'trudeau': ('A', 'World Politics'),
        'starmer': ('A', 'World Politics'),
        'modi': ('A', 'World Politics'),
        'pope': ('B', 'Religion'),
        'pontiff': ('B', 'Religion'),
        'vatican': ('B', 'Religion'),

        # === CURRENT NEWS FIGURES ===
        'daniel penny': ('A', 'Current Events'),
        'luigi mangione': ('A', 'Current Events'),
        'sam bankman': ('A', 'Current Events'),
        'sbf': ('A', 'Current Events'),
        'caroline ellison': ('B', 'Current Events'),

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
        'niners': ('A', 'NFL Team'),
        'ravens': ('A', 'NFL Team'),
        'bills': ('A', 'NFL Team'),
        'lions': ('A', 'NFL Team'),
        'packers': ('A', 'NFL Team'),
        'dolphins': ('A', 'NFL Team'),
        'bengals': ('A', 'NFL Team'),
        'steelers': ('A', 'NFL Team'),
        'broncos': ('A', 'NFL Team'),
        'raiders': ('A', 'NFL Team'),
        'chargers': ('A', 'NFL Team'),
        'jets': ('A', 'NFL Team'),
        'giants': ('A', 'NFL Team'),
        'patriots': ('A', 'NFL Team'),
        'seahawks': ('A', 'NFL Team'),
        'commanders': ('A', 'NFL Team'),
        'bears': ('A', 'NFL Team'),
        'vikings': ('A', 'NFL Team'),
        'saints': ('A', 'NFL Team'),
        'falcons': ('A', 'NFL Team'),
        'buccaneers': ('A', 'NFL Team'),
        'bucs': ('A', 'NFL Team'),
        'panthers': ('A', 'NFL Team'),
        'cardinals': ('A', 'NFL Team'),
        'rams': ('A', 'NFL Team'),
        'texans': ('A', 'NFL Team'),
        'colts': ('A', 'NFL Team'),
        'jaguars': ('A', 'NFL Team'),
        'titans': ('A', 'NFL Team'),
        'browns': ('A', 'NFL Team'),
        'mahomes': ('S', 'Star Player'),
        'kelce': ('S', 'Star Player'),
        'travis kelce': ('S', 'Star Player'),
        'lamar jackson': ('A', 'Star Player'),
        'josh allen': ('A', 'Star Player'),
        'burrow': ('A', 'Star Player'),
        'joe burrow': ('A', 'Star Player'),
        'rodgers': ('A', 'Star Player'),
        'aaron rodgers': ('A', 'Star Player'),
        'dak prescott': ('A', 'Star Player'),
        'cj stroud': ('A', 'Star Player'),
        'hurts': ('A', 'Star Player'),
        'jalen hurts': ('A', 'Star Player'),
        'purdy': ('A', 'Star Player'),
        'brock purdy': ('A', 'Star Player'),
        'garrett wilson': ('A', 'Star Player'),
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
        'nets': ('A', 'NBA Team'),
        'bucks': ('A', 'NBA Team'),
        'sixers': ('A', 'NBA Team'),
        '76ers': ('A', 'NBA Team'),
        'suns': ('A', 'NBA Team'),
        'mavericks': ('A', 'NBA Team'),
        'mavs': ('A', 'NBA Team'),
        'nuggets': ('A', 'NBA Team'),
        'clippers': ('A', 'NBA Team'),
        'timberwolves': ('A', 'NBA Team'),
        'grizzlies': ('A', 'NBA Team'),
        'pelicans': ('A', 'NBA Team'),
        'spurs': ('A', 'NBA Team'),
        'thunder': ('A', 'NBA Team'),
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
        'jayson tatum': ('A', 'Star Player'),
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
        'red sox': ('A', 'MLB Team'),
        'cubs': ('A', 'MLB Team'),
        'white sox': ('A', 'MLB Team'),
        'phillies': ('A', 'MLB Team'),
        'braves': ('A', 'MLB Team'),
        'astros': ('A', 'MLB Team'),
        'padres': ('A', 'MLB Team'),
        'mariners': ('A', 'MLB Team'),
        'orioles': ('A', 'MLB Team'),
        'twins': ('A', 'MLB Team'),
        'guardians': ('A', 'MLB Team'),
        'royals': ('A', 'MLB Team'),
        'rangers': ('A', 'MLB Team'),
        'blue jays': ('A', 'MLB Team'),
        'rays': ('A', 'MLB Team'),
        'marlins': ('B', 'MLB Team'),
        'reds': ('B', 'MLB Team'),
        'pirates': ('B', 'MLB Team'),
        'rockies': ('B', 'MLB Team'),
        'diamondbacks': ('B', 'MLB Team'),
        'athletics': ('B', 'MLB Team'),
        'nationals': ('B', 'MLB Team'),
        'brewers': ('B', 'MLB Team'),
        'tigers': ('B', 'MLB Team'),
        'angels': ('B', 'MLB Team'),
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

        # === SPORTS: EPL/Soccer (Tier A) ===
        'epl': ('A', 'League'),
        'premier league': ('A', 'League'),
        'champions league': ('A', 'Major Event'),
        'world cup': ('S', 'Global Event'),
        'la liga': ('A', 'League'),
        'serie a': ('A', 'League'),
        'bundesliga': ('A', 'League'),
        'fa cup': ('A', 'Event'),
        # EPL Teams
        'manchester united': ('A', 'EPL Team'),
        'man united': ('A', 'EPL Team'),
        'man utd': ('A', 'EPL Team'),
        'manchester city': ('A', 'EPL Team'),
        'man city': ('A', 'EPL Team'),
        'liverpool': ('A', 'EPL Team'),
        'arsenal': ('A', 'EPL Team'),
        'chelsea': ('A', 'EPL Team'),
        'tottenham': ('A', 'EPL Team'),
        'spurs': ('A', 'EPL Team'),
        'newcastle': ('A', 'EPL Team'),
        'aston villa': ('A', 'EPL Team'),
        'west ham': ('A', 'EPL Team'),
        'brighton': ('A', 'EPL Team'),
        'brentford': ('B', 'EPL Team'),
        'wolves': ('B', 'EPL Team'),
        'wolverhampton': ('B', 'EPL Team'),
        'crystal palace': ('B', 'EPL Team'),
        'fulham': ('B', 'EPL Team'),
        'everton': ('B', 'EPL Team'),
        'nottingham': ('B', 'EPL Team'),
        'bournemouth': ('B', 'EPL Team'),
        'leicester': ('B', 'EPL Team'),
        'ipswich': ('B', 'EPL Team'),
        'southampton': ('B', 'EPL Team'),
        # EPL Players
        'salah': ('A', 'EPL Star'),
        'mohamed salah': ('A', 'EPL Star'),
        'haaland': ('A', 'Star Player'),
        'erling': ('A', 'Star Player'),
        'saka': ('A', 'EPL Star'),
        'bukayo saka': ('A', 'EPL Star'),
        'son': ('A', 'EPL Star'),
        'heung-min son': ('A', 'EPL Star'),
        'heung min son': ('A', 'EPL Star'),
        'kane': ('A', 'Star Player'),
        'harry kane': ('A', 'Star Player'),
        'rashford': ('A', 'EPL Star'),
        'bruno fernandes': ('A', 'EPL Star'),
        'de bruyne': ('A', 'EPL Star'),
        'foden': ('A', 'EPL Star'),
        'phil foden': ('A', 'EPL Star'),
        'palmer': ('A', 'EPL Star'),
        'cole palmer': ('A', 'EPL Star'),
        'jamie vardy': ('A', 'EPL Star'),
        'vardy': ('A', 'EPL Star'),
        'diogo jota': ('A', 'EPL Star'),
        'jota': ('A', 'EPL Star'),
        # Other soccer
        'messi': ('S', 'Global Star'),
        'lionel messi': ('S', 'Global Star'),
        'ronaldo': ('S', 'Global Star'),
        'cristiano': ('S', 'Global Star'),
        'mbappe': ('S', 'Star Player'),
        'kylian': ('S', 'Star Player'),
        'bellingham': ('A', 'Star Player'),
        'jude bellingham': ('A', 'Star Player'),
        'yamal': ('A', 'Star Player'),
        'neymar': ('A', 'Star Player'),
        'barcelona': ('A', 'Soccer'),
        'real madrid': ('A', 'Soccer'),
        'psg': ('A', 'Soccer'),
        'bayern': ('A', 'Soccer'),
        'juventus': ('A', 'Soccer'),
        'inter milan': ('A', 'Soccer'),
        'ac milan': ('A', 'Soccer'),
        'relegated': ('B', 'Soccer'),
        'relegation': ('B', 'Soccer'),
        'goalscorer': ('B', 'Soccer'),

        # === SPORTS: UFC/MMA (Tier A) ===
        'ufc': ('A', 'Combat Sports'),
        'mma': ('A', 'Combat Sports'),
        'bellator': ('B', 'Combat Sports'),
        'pfl': ('B', 'Combat Sports'),
        # UFC Fighters
        'mcgregor': ('A', 'Star Fighter'),
        'conor mcgregor': ('A', 'Star Fighter'),
        'jon jones': ('A', 'Star Fighter'),
        'jones': ('B', 'Fighter'),
        'adesanya': ('A', 'Star Fighter'),
        'izzy': ('A', 'Star Fighter'),
        'usman': ('A', 'Star Fighter'),
        'khabib': ('A', 'Star Fighter'),
        'holloway': ('A', 'Star Fighter'),
        'max holloway': ('A', 'Star Fighter'),
        'topuria': ('A', 'Star Fighter'),
        'ilia topuria': ('A', 'Star Fighter'),
        'volkanovski': ('A', 'Star Fighter'),
        'volk': ('A', 'Star Fighter'),
        'o\'malley': ('A', 'Star Fighter'),
        'sean o\'malley': ('A', 'Star Fighter'),
        'pereira': ('A', 'Star Fighter'),
        'alex pereira': ('A', 'Star Fighter'),
        'chimaev': ('A', 'Star Fighter'),
        'khamzat': ('A', 'Star Fighter'),
        'blanchfield': ('B', 'Fighter'),
        'barber': ('B', 'Fighter'),
        'ponzinibbio': ('B', 'Fighter'),
        'machado garry': ('B', 'Fighter'),

        # === SPORTS: Other ===
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
        'australian open': ('A', 'Tennis'),
        'french open': ('A', 'Tennis'),
        'verstappen': ('A', 'Star Driver'),
        'max verstappen': ('A', 'Star Driver'),
        'hamilton': ('A', 'Star Driver'),
        'lewis hamilton': ('A', 'Star Driver'),
        'tiger woods': ('A', 'Golf Legend'),
        'djokovic': ('A', 'Tennis Star'),
        'nadal': ('A', 'Tennis Star'),
        'federer': ('A', 'Tennis Star'),
        'sinner': ('A', 'Tennis Star'),
        'jannik sinner': ('A', 'Tennis Star'),
        'alcaraz': ('A', 'Tennis Star'),
        'carlos alcaraz': ('A', 'Tennis Star'),

        # === College Sports ===
        'ncaa': ('A', 'College Sports'),
        'march madness': ('S', 'College Event'),
        'college football': ('A', 'College Sports'),
        'cfp': ('A', 'College Football'),
        'clemson': ('A', 'College Team'),
        'alabama': ('A', 'College Team'),
        'georgia': ('A', 'College Team'),
        'ohio state': ('A', 'College Team'),
        'michigan': ('A', 'College Team'),
        'texas': ('A', 'College Team'),
        'oregon': ('A', 'College Team'),
        'notre dame': ('A', 'College Team'),
        'lsu': ('A', 'College Team'),
        'usc': ('A', 'College Team'),
        'florida': ('A', 'College Team'),
        'penn state': ('A', 'College Team'),
        'tennessee': ('A', 'College Team'),
        'oklahoma': ('A', 'College Team'),

        # === MUSIC & POP CULTURE (Tier S/A) ===
        'taylor swift': ('S', 'Megastar'),
        'swift': ('S', 'Megastar'),
        'beyonce': ('S', 'Megastar'),
        'beyoncé': ('S', 'Megastar'),
        'drake': ('S', 'Megastar'),
        'kendrick': ('S', 'Megastar'),
        'kendrick lamar': ('S', 'Megastar'),
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

        # === GAMING (Tier S/A) ===
        'gta': ('S', 'Major Game'),
        'gta 6': ('S', 'Major Game'),
        'gta vi': ('S', 'Major Game'),
        'grand theft auto': ('S', 'Major Game'),
        'call of duty': ('A', 'Major Game'),
        'fortnite': ('A', 'Major Game'),
        'nintendo': ('A', 'Gaming'),
        'playstation': ('A', 'Gaming'),
        'xbox': ('A', 'Gaming'),

        # === TECH (Tier A/B) ===
        'apple': ('A', 'Tech'),
        'iphone': ('A', 'Tech'),
        'google': ('A', 'Tech'),
        'microsoft': ('A', 'Tech'),
        'tesla': ('A', 'Tech'),
        'openai': ('A', 'Tech'),
        'chatgpt': ('A', 'Tech'),
        'spacex': ('A', 'Tech'),
        'twitter': ('A', 'Tech'),
        'tiktok': ('A', 'Tech'),

        # === ECONOMY (Tier B) ===
        'fed': ('B', 'Economy'),
        'federal reserve': ('B', 'Economy'),
        'interest rate': ('B', 'Economy'),
        'rate cut': ('B', 'Economy'),
        'inflation': ('B', 'Economy'),
        'recession': ('B', 'Economy'),

        # === CRYPTO (Tier A/B) ===
        'bitcoin': ('A', 'Crypto'),
        'btc': ('A', 'Crypto'),
        'ethereum': ('A', 'Crypto'),
        'eth': ('A', 'Crypto'),
        'dogecoin': ('A', 'Crypto'),
        'doge': ('A', 'Crypto'),
        'solana': ('B', 'Crypto'),
        'xrp': ('B', 'Crypto'),
        'crypto': ('B', 'Crypto'),
        'coinbase': ('B', 'Crypto'),
        'kraken': ('B', 'Crypto'),
    }

    IRRELEVANT_KEYWORDS = {
        # Weather
        'weather', 'temperature', 'snowfall', 'rainfall', 'rain', 'snow',
        'humidity', 'celsius', 'fahrenheit',
        # Local/Municipal
        'local', 'municipal', 'county', 'sheriff', 'mayor', 'city council',
        'subway', 'transit', 'bus route',
        # Prop bets / Measures
        'measure', 'proposition',
        # Meta/Platform
        'daily active users', 'friend.tech', 'manifold', 'metaculus',
        'polymarket volume', 'kalshi volume',
        # Natural disasters (not sports/entertainment)
        'earthquake', 'megaquake', 'tsunami', 'hurricane',
        'volcano', 'eruption',
        # Nuclear/War
        'nuclear weapon', 'nuclear detonation',
        # Meme/Obscure
        'grimace coin', 'grimace vs',
    }

    def __init__(self):
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.warning("OPENAI_API_KEY not set - using fallback relevance checking")
            self.client = None
        else:
            self.client = OpenAI(api_key=api_key)
        
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
                    'reasoning': f'[RULE:SKIP] {keyword}',
                    'topic': topic
                }
                logger.debug(f"SKIP: {market_title[:50]}...")
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
                return cache_and_return(result)
        
        # 4. Use AI for everything else
        if self.client:
            self.ai_call_count += 1
            logger.info(f"[AI #{self.ai_call_count}] {market_title[:80]}...")
            result = self._check_with_ai(market_title, topic)
            logger.info(f"[AI RESULT] rel={result['is_relevant']}, tier={result['tier']}")
            return cache_and_return(result)
        else:
            return {
                'is_relevant': True,
                'tier': 'B',
                'reasoning': '[FALLBACK] AI unavailable',
                'topic': topic
            }
    
    def _extract_topic(self, title: str) -> str:
        title = re.sub(r'will\s+', '', title, flags=re.IGNORECASE)
        title = re.sub(r'\?', '', title)
        words = title.strip().split()[:4]
        return ' '.join(words)
    
    def _check_with_ai(self, market_title: str, topic: str) -> Dict:
        try:
            prompt = f"""Is this prediction market relevant for sports/entertainment betting sites?

Market: "{market_title}"

Reply in exactly this format:
RELEVANT: YES or NO
TIER: S (A-list celebrity/major event), A (popular), or C (niche)
REASON: one sentence"""

            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=60,
                temperature=0.1
            )
            
            text = response.choices[0].message.content
            is_relevant = 'YES' in text.split('\n')[0].upper()
            
            tier_match = re.search(r'TIER:\s*([SAC])', text, re.IGNORECASE)
            tier = tier_match.group(1).upper() if tier_match else 'B'
            
            reason_match = re.search(r'REASON:\s*(.+)', text, re.IGNORECASE)
            reasoning = reason_match.group(1).strip() if reason_match else 'AI analysis'
            
            return {
                'is_relevant': is_relevant,
                'tier': tier,
                'reasoning': f'[AI] {reasoning}',
                'topic': topic
            }
            
        except Exception as e:
            logger.error(f"AI error: {e}")
            return {
                'is_relevant': True,
                'tier': 'B',
                'reasoning': '[AI ERROR]',
                'topic': topic
            }