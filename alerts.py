import os
import json
import requests
from enum import Enum
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime
import logging

from models import AlertLog, DigestQueue

logger = logging.getLogger(__name__)


class AlertTier(Enum):
    URGENT = "urgent"
    DAILY = "daily"
    BACKGROUND = "background"


@dataclass
class HotMarket:
    market: any
    tier: AlertTier
    signals: List[str]
    context: Dict


class AlertManager:
    
    def __init__(self):
        self.slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        self.max_alerts_per_hour = int(os.getenv('MAX_ALERTS_PER_HOUR', '5'))
        self.business_hours_only = os.getenv('BUSINESS_HOURS_ONLY', 'true').lower() == 'true'
        
        if not self.slack_webhook_url:
            logger.warning("SLACK_WEBHOOK_URL not set - alerts will be logged only")
    
    # === URL HELPERS ===
    
    def _get_market_url(self, market=None, platform: str = None, raw_data: dict = None) -> Optional[str]:
        """Generate URL - works with market object OR stored data"""
        try:
            if market:
                platform = market.platform
                raw_data = market.raw_data
            
            if not platform or not raw_data:
                return None
            
            if platform == 'polymarket':
                slug = raw_data.get('slug')
                if slug:
                    return f"https://polymarket.com/event/{slug}"
                cid = raw_data.get('condition_id')
                if cid:
                    return f"https://polymarket.com/event/{cid}"
            
            elif platform == 'kalshi':
                ticker = raw_data.get('ticker', '')
                if ticker:
                    return f"https://kalshi.com/markets/{ticker.lower()}"
            
            return None
        except Exception:
            return None
    
    def _get_market_details(self, market=None, platform: str = None, raw_data: dict = None) -> Dict:
        """Extract details - works with market object OR stored data"""
        details = {
            'description': None,
            'rules': None,
            'outcomes': [],
            'liquidity': None,
            'open_interest': None,
            'spread': None,
            'source': None,
            'end_date': None,
        }
        
        if market:
            platform = market.platform
            raw_data = market.raw_data
        
        if not raw_data:
            return details
        
        raw = raw_data
        
        if platform == 'polymarket':
            desc = raw.get('description', '')
            if desc and len(desc) > 10:
                details['description'] = desc[:400] + ('...' if len(desc) > 400 else '')
            
            outcomes = raw.get('outcomes', [])
            prices = raw.get('outcomePrices', [])
            if outcomes and len(outcomes) > 2:
                outcome_list = []
                for i, outcome in enumerate(outcomes[:5]):
                    price = float(prices[i]) * 100 if i < len(prices) else 0
                    outcome_list.append({'name': outcome, 'odds': price})
                details['outcomes'] = sorted(outcome_list, key=lambda x: x['odds'], reverse=True)
            
            if raw.get('liquidity'):
                try:
                    details['liquidity'] = float(raw['liquidity'])
                except:
                    pass
            
            details['source'] = raw.get('resolutionSource')
            details['end_date'] = raw.get('end_date_iso')
            
        elif platform == 'kalshi':
            if raw.get('subtitle'):
                details['description'] = raw['subtitle']
            
            rules = raw.get('rules_primary', '')
            if rules:
                rules = rules.replace('\n', ' ').strip()
                details['rules'] = rules[:300] + ('...' if len(rules) > 300 else '')
            
            if raw.get('open_interest'):
                try:
                    details['open_interest'] = int(raw['open_interest'])
                except:
                    pass
            
            yes_bid = raw.get('yes_bid')
            yes_ask = raw.get('yes_ask')
            if yes_bid is not None and yes_ask is not None:
                details['spread'] = yes_ask - yes_bid
            
            details['source'] = raw.get('settlement_source_url')
            details['end_date'] = raw.get('expiration_time')
        
        return details
    
    # === FORMATTERS ===
    
    def _format_odds_from_context(self, context: dict) -> str:
        """Format odds from stored context"""
        odds = context.get('odds')
        if odds is None:
            return "N/A"
        pct = odds * 100
        if pct >= 80:
            return f"🟢 {pct:.0f}%"
        elif pct <= 20:
            return f"🔴 {pct:.0f}%"
        return f"🟡 {pct:.0f}%"
    
    def _format_odds(self, market) -> str:
        if not market.current_odds:
            return "N/A"
        yes_odds = market.current_odds.get('yes')
        if yes_odds is not None:
            pct = yes_odds * 100
            if pct >= 80:
                return f"🟢 {pct:.0f}%"
            elif pct <= 20:
                return f"🔴 {pct:.0f}%"
            return f"🟡 {pct:.0f}%"
        return "N/A"
    
    def _format_volume(self, volume: float) -> str:
        if volume >= 1_000_000:
            return f"${volume/1_000_000:.1f}M"
        elif volume >= 1_000:
            return f"${volume/1_000:.0f}K"
        return f"${volume:.0f}"
    
    def _format_number(self, num: float) -> str:
        if num >= 1_000_000:
            return f"{num/1_000_000:.1f}M"
        elif num >= 1_000:
            return f"{num/1_000:.0f}K"
        return f"{num:.0f}"
    
    # === ALERT SENDING ===
    
    def send_urgent_alerts(self, hot_markets: List[HotMarket]) -> None:
        if self.business_hours_only and not self._is_business_hours():
            logger.info("Outside business hours - queuing for digest")
            for hm in hot_markets:
                self._queue_market(hm)
            return
        
        recent_count = AlertLog.get_recent_alert_count(hours=1)
        if recent_count >= self.max_alerts_per_hour:
            logger.warning(f"Rate limit ({recent_count}/{self.max_alerts_per_hour}) - queuing")
            for hm in hot_markets:
                self._queue_market(hm)
            return
        
        for hm in hot_markets:
            # Skip if alerted recently
            if AlertLog.was_alerted_recently(hm.market.id, hours=6):
                logger.info(f"Already alerted: {hm.market.id}")
                continue
            
            message = self._format_urgent_alert(hm)
            success, slack_ts = self._send_to_slack(message)
            
            if success:
                AlertLog.create(
                    market_id=hm.market.id,
                    market_title=hm.market.title,
                    tier=hm.tier.value,
                    alert_type='real_time',
                    signals=hm.signals,
                    slack_ts=slack_ts
                )
                logger.info(f"Sent alert: {hm.market.title[:50]}")
            
            if AlertLog.get_recent_alert_count(hours=1) >= self.max_alerts_per_hour:
                logger.warning("Rate limit reached")
                break
    
    def _queue_market(self, hm: HotMarket) -> None:
        """Queue a market for digest with full context"""
        # Skip if already sent in last 24h
        if DigestQueue.was_recently_sent(hm.market.id, hours=24):
            logger.debug(f"Already in recent digest: {hm.market.id}")
            return
        
        DigestQueue.add_to_queue(
            market_id=hm.market.id,
            market_title=hm.market.title,
            tier=hm.tier.value,
            signals=hm.signals,
            context=hm.context,
            platform=hm.market.platform,
            raw_data=hm.market.raw_data
        )
        logger.info(f"Queued: {hm.market.title[:50]}")
    
    def queue_for_digest(self, hot_markets: List[HotMarket]) -> None:
        for hm in hot_markets:
            self._queue_market(hm)
    
    def send_digest(self, digest_type: str = 'morning') -> None:
        urgent = DigestQueue.get_queued_markets(tier='urgent')
        daily = DigestQueue.get_queued_markets(tier='daily')
        
        if not urgent and not daily:
            logger.info("No markets in digest queue")
            return
        
        # Filter out any that were already alerted as real-time
        urgent = [m for m in urgent if not AlertLog.was_alerted_recently(m.market_id, hours=12)]
        daily = [m for m in daily if not AlertLog.was_alerted_recently(m.market_id, hours=12)]
        
        if not urgent and not daily:
            logger.info("All queued markets were already alerted")
            return
        
        message = self._format_digest(urgent, daily, digest_type)
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
                    signals=json.loads(m.signals)
                )
            
            logger.info(f"Sent {digest_type} digest: {len(all_ids)} markets")
    
    # === MESSAGE FORMATTING ===
    
    def _format_urgent_alert(self, hm: HotMarket) -> Dict:
        market = hm.market
        context = hm.context
        
        market_url = self._get_market_url(market=market)
        details = self._get_market_details(market=market)
        
        # Build signal lines
        signal_lines = []
        for sig in hm.signals:
            if 'volume_spike' in sig:
                pct = context.get('volume_spike_1h', context.get('volume_spike_6h', 0)) * 100
                signal_lines.append(f"📈 *Volume:* +{pct:.0f}%")
            elif 'sustained_volume' in sig:
                pct = context.get('volume_spike_6h', 0) * 100
                signal_lines.append(f"📈 *6h Growth:* +{pct:.0f}%")
            elif 'odds' in sig:
                pts = context.get('odds_movement_1h', context.get('odds_movement_6h', 0)) * 100
                signal_lines.append(f"🎯 *Odds:* ±{abs(pts):.0f}pts")
            elif 'new' in sig:
                signal_lines.append(f"🆕 *New:* High-profile launch")
            elif 'event' in sig:
                signal_lines.append(f"📅 *Event:* Coming soon")
        
        # Title with link
        title = f"<{market_url}|{market.title}>" if market_url else f"*{market.title}*"
        
        tier = context.get('topic_tier', 'B')
        tier_emoji = "🔥" if tier == 'S' else "⭐" if tier == 'A' else "📊"
        
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"🚨 {tier}-Tier Alert", "emoji": True}
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": title}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*💰 Volume:*\n{self._format_volume(market.volume)}"},
                    {"type": "mrkdwn", "text": f"*🎲 Odds:*\n{self._format_odds(market)}"},
                    {"type": "mrkdwn", "text": f"*🏢 Platform:*\n{market.platform.title()}"},
                    {"type": "mrkdwn", "text": f"*{tier_emoji} Tier:*\n{tier}"}
                ]
            }
        ]
        
        if signal_lines:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Why:* " + " | ".join(signal_lines)}
            })
        
        if details.get('description'):
            desc = details['description'][:200] + ('...' if len(details['description']) > 200 else '')
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"_{desc}_"}]
            })
        
        if market_url:
            blocks.append({
                "type": "actions",
                "elements": [{
                    "type": "button",
                    "text": {"type": "plain_text", "text": f"View →", "emoji": True},
                    "url": market_url,
                    "style": "primary"
                }]
            })
        
        blocks.append({"type": "divider"})
        
        return {"blocks": blocks, "text": f"🚨 {tier}: {market.title}"}
    
    def _format_digest(self, urgent: List, daily: List, digest_type: str) -> Dict:
        """Format digest with links and full context"""
        
        title = "☀️ MORNING DIGEST" if digest_type == 'morning' else "🌙 EVENING DIGEST"
        total = len(urgent) + len(daily)
        
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{title}", "emoji": True}
            },
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"📅 {datetime.now().strftime('%B %d, %Y')} • *{total}* markets"}]
            },
            {"type": "divider"}
        ]
        
        # Urgent markets
        if urgent:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*🔥 URGENT ({len(urgent)})*"}
            })
            
            for item in urgent[:5]:
                block = self._format_digest_item(item, show_full=True)
                blocks.extend(block)
            
            blocks.append({"type": "divider"})
        
        # Daily markets
        if daily:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*📈 OPPORTUNITIES ({len(daily)})*"}
            })
            
            for item in daily[:10]:
                block = self._format_digest_item(item, show_full=False)
                blocks.extend(block)
        
        if not urgent and not daily:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "No hot markets today. 😴"}
            })
        
        return {"blocks": blocks, "text": f"{title} - {total} markets"}
    
    def _format_digest_item(self, item, show_full: bool = False) -> List[Dict]:
        """Format a single digest item with link"""
        blocks = []
        
        ctx = json.loads(item.context) if item.context else {}
        signals = json.loads(item.signals) if item.signals else []
        raw = json.loads(item.raw_data) if item.raw_data else {}
        
        # Generate URL
        url = self._get_market_url(platform=item.platform, raw_data=raw)
        
        # Title with link
        title = item.market_title[:60]
        if len(item.market_title) > 60:
            title += "..."
        
        if url:
            title_text = f"<{url}|{title}>"
        else:
            title_text = f"*{title}*"
        
        # Build signal summary
        signal_parts = []
        for sig in signals:
            if 'volume' in sig:
                pct = ctx.get('volume_spike_1h', ctx.get('volume_spike_6h', 0)) * 100
                if pct > 0:
                    signal_parts.append(f"📈+{pct:.0f}%")
            elif 'odds' in sig:
                signal_parts.append("🎯 Odds")
            elif 'event' in sig:
                signal_parts.append("📅 Soon")
        
        volume = ctx.get('volume', 0)
        odds = ctx.get('odds')
        
        # Format line
        info_parts = [self._format_volume(volume)]
        if odds:
            info_parts.append(self._format_odds_from_context(ctx))
        if signal_parts:
            info_parts.append(" ".join(signal_parts[:2]))
        
        info_line = " • ".join(info_parts)
        
        if show_full:
            # Two-line format for urgent
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"• {title_text}\n   {info_line}"}
            })
            
            # Add description if available
            details = self._get_market_details(platform=item.platform, raw_data=raw)
            if details.get('description') and show_full:
                desc = details['description'][:150]
                blocks.append({
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": f"   _{desc}_"}]
                })
        else:
            # Single line for daily
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"• {title_text} — {info_line}"}
            })
        
        return blocks
    
    def _send_to_slack(self, message: Dict) -> tuple[bool, Optional[str]]:
        if not self.slack_webhook_url:
            logger.info(f"[DRY RUN] {json.dumps(message, indent=2)}")
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
