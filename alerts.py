"""
Alert Manager - Event-First Architecture

Sends alerts about HOT EVENTS (not individual markets)
with top constituent markets listed.
"""

import os
import json
import requests
import re
from enum import Enum
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime
import logging

from models import AlertLog, DigestQueue
from clustering import MarketCluster
from market import Market

logger = logging.getLogger(__name__)


class AlertTier(Enum):
    URGENT = "urgent"
    DAILY = "daily"
    BACKGROUND = "background"


@dataclass
class HotEvent:
    """Represents a hot event cluster"""
    cluster: MarketCluster
    tier: AlertTier
    topic_tier: str
    signals: Dict
    top_markets: List[Market]
    context: Dict


class AlertManager:
    KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
    
    def __init__(self):
        self.slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        self.max_alerts_per_hour = int(os.getenv('MAX_ALERTS_PER_HOUR', '5'))
        self.business_hours_only = os.getenv('BUSINESS_HOURS_ONLY', 'false').lower() == 'true'
        self._kalshi_event_cache = {}
        self._kalshi_series_title_cache = {}
        
        if not self.slack_webhook_url:
            logger.warning("SLACK_WEBHOOK_URL not set - alerts logged only")
    
    # === URL HELPERS ===

    def _slugify_kalshi_title(self, title: str) -> str:
        slug = title.strip().lower()
        slug = re.sub(r"[^a-z0-9\\s-]", "", slug)
        slug = re.sub(r"[\\s_-]+", "-", slug)
        return slug.strip("-")

    def _get_kalshi_event(self, event_ticker: str) -> Optional[Dict]:
        cached = self._kalshi_event_cache.get(event_ticker)
        if cached is not None:
            return cached
        try:
            resp = requests.get(
                f"{self.KALSHI_API_BASE}/events/{event_ticker}",
                timeout=5
            )
            if resp.status_code != 200:
                self._kalshi_event_cache[event_ticker] = None
                return None
            event = (resp.json() or {}).get("event")
            self._kalshi_event_cache[event_ticker] = event
            return event
        except Exception:
            self._kalshi_event_cache[event_ticker] = None
            return None

    def _get_kalshi_series_title(self, series_ticker: str) -> Optional[str]:
        cached = self._kalshi_series_title_cache.get(series_ticker)
        if cached is not None:
            return cached
        try:
            resp = requests.get(
                f"{self.KALSHI_API_BASE}/series/{series_ticker}",
                timeout=5
            )
            if resp.status_code != 200:
                self._kalshi_series_title_cache[series_ticker] = None
                return None
            title = (resp.json() or {}).get("series", {}).get("title")
            self._kalshi_series_title_cache[series_ticker] = title
            return title
        except Exception:
            self._kalshi_series_title_cache[series_ticker] = None
            return None

    def _build_kalshi_event_url(self, raw: Dict) -> Optional[str]:
        event_ticker = raw.get("event_ticker") or raw.get("ticker")
        if not event_ticker:
            return None

        event = self._get_kalshi_event(event_ticker)
        series_ticker = (event or {}).get("series_ticker") or raw.get("series_ticker")
        if not series_ticker:
            return None

        series_title = self._get_kalshi_series_title(series_ticker)
        if not series_title:
            return None

        series_slug = self._slugify_kalshi_title(series_title)
        if not series_slug:
            return None

        return (
            f"https://kalshi.com/markets/"
            f"{series_ticker.lower()}/{series_slug}/{event_ticker.lower()}"
        )
    
    def _get_market_url(self, market: Market) -> Optional[str]:
        try:
            raw = market.raw_data
            
            if market.platform == 'polymarket':
                slug = raw.get('slug')
                if slug:
                    return f"https://polymarket.com/event/{slug}"
                cid = raw.get('condition_id')
                if cid:
                    return f"https://polymarket.com/event/{cid}"
            
            elif market.platform == 'kalshi':
                return self._build_kalshi_event_url(raw)
            
            return None
        except Exception:
            return None
    
    # === FORMATTERS ===
    
    def _format_volume(self, volume: float) -> str:
        if volume >= 1_000_000:
            return f"${volume/1_000_000:.1f}M"
        elif volume >= 1_000:
            return f"${volume/1_000:.0f}K"
        return f"${volume:.0f}"
    
    def _format_odds(self, market: Market) -> str:
        if not market.current_odds:
            return ""
        yes_odds = market.current_odds.get('yes')
        if yes_odds is not None:
            pct = yes_odds * 100
            if pct >= 80:
                return f"🟢 {pct:.0f}%"
            elif pct <= 20:
                return f"🔴 {pct:.0f}%"
            return f"🟡 {pct:.0f}%"
        return ""
    
    def _get_signal_emoji(self, signal: str) -> str:
        if 'volume_spike' in signal:
            return "📈"
        elif 'event_in' in signal:
            return "📅"
        elif 'multi_platform' in signal:
            return "🌐"
        elif 'high_volume' in signal:
            return "💰"
        elif 'active_event' in signal:
            return "🔥"
        return "📊"
    
    # === EVENT ALERTS ===
    
    def send_urgent_event_alerts(self, hot_events: List[HotEvent]) -> None:
        """Send urgent alerts for hot events"""
        
        if self.business_hours_only and not self._is_business_hours():
            logger.info("Outside business hours - queuing for digest")
            self.queue_events_for_digest(hot_events)
            return
        
        recent_count = AlertLog.get_recent_alert_count(hours=1)
        if recent_count >= self.max_alerts_per_hour:
            logger.warning(f"Rate limit hit - queuing")
            self.queue_events_for_digest(hot_events)
            return
        
        for event in hot_events:
            # Use event_id for dedup
            event_id = event.cluster.event_id
            
            if AlertLog.was_alerted_recently(event_id, hours=6):
                logger.info(f"Already alerted: {event_id}")
                continue
            
            message = self._format_event_alert(event)
            success, slack_ts = self._send_to_slack(message)
            
            if success:
                AlertLog.create(
                    market_id=event_id,
                    market_title=event.cluster.title,
                    tier=event.tier.value,
                    alert_type='real_time',
                    signals=event.signals.get('triggered', []),
                    slack_ts=slack_ts
                )
                logger.info(f"Sent event alert: {event.cluster.title[:50]}")
            
            if AlertLog.get_recent_alert_count(hours=1) >= self.max_alerts_per_hour:
                break
    
    def queue_events_for_digest(self, hot_events: List[HotEvent]) -> None:
        """Queue events for digest"""
        for event in hot_events:
            event_id = event.cluster.event_id

            # Skip if recently sent in digest OR recently alerted
            if DigestQueue.was_recently_sent(event_id, hours=24):
                continue
            if AlertLog.was_alerted_recently(event_id, hours=24):
                continue
            
            # Store primary market data for URL generation
            primary = event.cluster.primary_market
            
            DigestQueue.add_to_queue(
                market_id=event_id,
                market_title=event.cluster.title,
                tier=event.tier.value,
                signals=event.signals.get('triggered', []),
                context={
                    **event.context,
                    'top_markets': [
                        {
                            'title': m.title,
                            'volume': m.volume,
                            'platform': m.platform,
                            'odds': m.current_odds.get('yes') if m.current_odds else None
                        }
                        for m in event.top_markets
                    ]
                },
                platform=primary.platform,
                raw_data=primary.raw_data
            )
            logger.info(f"Queued event: {event.cluster.title[:50]}")
    
    def send_digest(self, digest_type: str = 'morning') -> None:
        """Send scheduled digest"""
        urgent = DigestQueue.get_queued_markets(tier='urgent')
        daily = DigestQueue.get_queued_markets(tier='daily')
        
        if not urgent and not daily:
            logger.info("No events in digest queue")
            return
        
        # Filter already alerted (24 hours to prevent same markets in consecutive digests)
        urgent = [m for m in urgent if not AlertLog.was_alerted_recently(m.market_id, hours=24)]
        daily = [m for m in daily if not AlertLog.was_alerted_recently(m.market_id, hours=24)]
        
        if not urgent and not daily:
            logger.info("All queued events were already alerted")
            return
        
        message = self._format_event_digest(urgent, daily, digest_type)
        success, _ = self._send_to_slack(message)
        
        if success:
            all_ids = [m.market_id for m in urgent + daily]
            DigestQueue.mark_as_sent(all_ids)
            
            for m in urgent + daily:
                AlertLog.create(
                    market_id=m.market_id,
                    market_title=m.market_title,
                    tier=m.alert_tier,
                    alert_type='digest',
                    signals=json.loads(m.signals) if m.signals else []
                )
            
            logger.info(f"Sent {digest_type} digest: {len(all_ids)} events")
    
    # === MESSAGE FORMATTING ===
    
    def _format_event_alert(self, event: HotEvent) -> Dict:
        """Format an event-level alert"""
        
        cluster = event.cluster
        tier_emoji = "🔥" if event.topic_tier == 'S' else "⭐" if event.topic_tier == 'A' else "📊"
        
        # Header
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🚨 HOT EVENT: {cluster.title[:50]}",
                    "emoji": True
                }
            }
        ]
        
        # Event summary
        platforms = " / ".join(cluster.platform_spread).title()
        summary_text = (
            f"*Total Volume:* {self._format_volume(cluster.total_volume)} | "
            f"*Markets:* {cluster.market_count} | "
            f"*Platforms:* {platforms}"
        )
        
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": summary_text}
        })
        
        # Signals
        triggered = event.signals.get('triggered', [])
        if triggered:
            signal_parts = [f"{self._get_signal_emoji(s)} {s.replace('_', ' ').title()}" for s in triggered[:4]]
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": " • ".join(signal_parts)}]
            })
        
        blocks.append({"type": "divider"})
        
        # Top Markets section
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*📋 Top Markets:*"}
        })
        
        for i, market in enumerate(event.top_markets[:3], 1):
            url = self._get_market_url(market)
            title = market.title[:55] + ('...' if len(market.title) > 55 else '')
            
            if url:
                market_line = f"<{url}|{title}>"
            else:
                market_line = f"*{title}*"
            
            odds_str = self._format_odds(market)
            vol_str = self._format_volume(market.volume)
            platform_badge = f"[{market.platform.title()}]"
            
            info_line = f"{vol_str}"
            if odds_str:
                info_line += f" • {odds_str}"
            
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"  {i}. {market_line} {platform_badge}\n      {info_line}"}
            })
        
        # View button for primary market
        primary_url = self._get_market_url(cluster.primary_market)
        if primary_url:
            blocks.append({
                "type": "actions",
                "elements": [{
                    "type": "button",
                    "text": {"type": "plain_text", "text": f"View on {cluster.primary_market.platform.title()} →", "emoji": True},
                    "url": primary_url,
                    "style": "primary"
                }]
            })
        
        blocks.append({"type": "divider"})
        
        return {
            "blocks": blocks,
            "text": f"🚨 HOT EVENT: {cluster.title} ({self._format_volume(cluster.total_volume)})"
        }
    
    def _format_event_digest(self, urgent: List, daily: List, digest_type: str) -> Dict:
        """Format event digest"""
        
        title = "☀️ MORNING DIGEST" if digest_type == 'morning' else "🌙 EVENING DIGEST"
        total = len(urgent) + len(daily)
        
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": title, "emoji": True}
            },
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"📅 {datetime.now().strftime('%B %d, %Y')} • *{total}* hot events"}]
            },
            {"type": "divider"}
        ]
        
        if urgent:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*🔥 URGENT ({len(urgent)})*"}
            })
            
            for item in urgent[:5]:
                blocks.extend(self._format_digest_event_item(item, show_markets=True))
            
            blocks.append({"type": "divider"})
        
        if daily:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*📈 TODAY'S OPPORTUNITIES ({len(daily)})*"}
            })
            
            for item in daily[:10]:
                blocks.extend(self._format_digest_event_item(item, show_markets=False))
        
        return {"blocks": blocks, "text": f"{title} - {total} events"}
    
    def _format_digest_event_item(self, item, show_markets: bool = False) -> List[Dict]:
        """Format a single event in digest"""
        blocks = []
        
        ctx = json.loads(item.context) if item.context else {}
        signals = json.loads(item.signals) if item.signals else []
        
        # Parse raw_data for URL
        try:
            raw = json.loads(item.raw_data) if item.raw_data else {}
        except:
            raw = {}

        # Log if raw_data is missing
        if not raw:
            logger.warning(f"No raw_data for market: {item.market_title[:50]} - links will be broken")

        url = None
        platform = item.platform
        if platform == 'polymarket':
            # Try multiple URL patterns for Polymarket
            slug = raw.get('slug')
            condition_id = raw.get('condition_id')

            if slug:
                # Primary: use slug with /event/ path
                url = f"https://polymarket.com/event/{slug}"
            elif condition_id:
                # Fallback: use condition_id
                url = f"https://polymarket.com/event/{condition_id}"
            else:
                logger.warning(f"No slug or condition_id for Polymarket market: {item.market_title[:50]}")
        elif platform == 'kalshi':
            url = self._build_kalshi_event_url(raw)
        
        # Title
        title = item.market_title[:50] + ('...' if len(item.market_title) > 50 else '')
        if url:
            title_text = f"<{url}|{title}>"
        else:
            title_text = f"*{title}*"
        
        # Stats
        vol = ctx.get('total_volume', 0)
        mkt_count = ctx.get('market_count', 1)
        platforms = ctx.get('platforms', [])
        
        info_parts = [self._format_volume(vol)]
        if mkt_count > 1:
            info_parts.append(f"{mkt_count} markets")
        if len(platforms) > 1:
            info_parts.append("Multi-platform")
        
        # Signal emoji
        signal_emoji = ""
        for s in signals[:2]:
            signal_emoji += self._get_signal_emoji(s)
        
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"• {title_text} {signal_emoji}\n   {' • '.join(info_parts)}"}
        })
        
        # Show top markets if requested
        if show_markets:
            top_markets = ctx.get('top_markets', [])[:3]
            if top_markets:
                market_lines = []
                for m in top_markets:
                    m_title = m['title'][:40] + ('...' if len(m['title']) > 40 else '')
                    m_vol = self._format_volume(m['volume'])
                    market_lines.append(f"   └ {m_title} ({m_vol})")
                
                blocks.append({
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": "\n".join(market_lines)}]
                })
        
        return blocks
    
    def _send_to_slack(self, message: Dict) -> tuple[bool, Optional[str]]:
        if not self.slack_webhook_url:
            logger.info(f"[DRY RUN] Would send: {message.get('text', '')[:100]}")
            return False, None
        
        try:
            resp = requests.post(
                self.slack_webhook_url,
                json=message,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            if resp.status_code == 200:
                return True, None
            logger.error(f"Slack error: {resp.status_code} - {resp.text}")
            return False, None
        except Exception as e:
            logger.error(f"Slack send error: {e}")
            return False, None
    
    def _is_business_hours(self) -> bool:
        now = datetime.now()
        return 9 <= now.hour < 18
