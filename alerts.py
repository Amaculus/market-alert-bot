
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
    """Alert priority tiers"""
    URGENT = "urgent"      # Real-time alerts
    DAILY = "daily"        # Digest opportunities
    BACKGROUND = "background"  # Research queue


@dataclass
class HotMarket:
    """Represents a hot market that triggered an alert"""
    market: any  # Market object from clustering_engine
    tier: AlertTier
    signals: List[str]
    context: Dict


class AlertManager:
    """Manages alert sending and rate limiting"""
    
    def __init__(self):
        self.slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        self.max_alerts_per_hour = int(os.getenv('MAX_ALERTS_PER_HOUR', '2'))
        self.business_hours_only = os.getenv('BUSINESS_HOURS_ONLY', 'true').lower() == 'true'
        
        if not self.slack_webhook_url:
            logger.warning("SLACK_WEBHOOK_URL not set - alerts will be logged only")
    
    def send_urgent_alerts(self, hot_markets: List[HotMarket]) -> None:
        """Send real-time urgent alerts with rate limiting"""
        
        # Check business hours
        if self.business_hours_only and not self._is_business_hours():
            logger.info("Outside business hours - queuing urgent alerts for morning digest")
            for market in hot_markets:
                DigestQueue.add_to_queue(
                    market_id=market.market.id,
                    market_title=market.market.title,
                    tier=market.tier.value,
                    signals=market.signals,
                    context=market.context
                )
            return
        
        # Check rate limit
        recent_count = AlertLog.get_recent_alert_count(hours=1)
        if recent_count >= self.max_alerts_per_hour:
            logger.warning(f"Rate limit reached ({recent_count}/{self.max_alerts_per_hour}) - queuing alerts")
            for market in hot_markets:
                DigestQueue.add_to_queue(
                    market_id=market.market.id,
                    market_title=market.market.title,
                    tier=market.tier.value,
                    signals=market.signals,
                    context=market.context
                )
            return
        
        # Send alerts
        for market in hot_markets:
            # Check if already alerted recently
            if AlertLog.was_alerted_recently(market.market.id, hours=6):
                logger.info(f"Market {market.market.id} already alerted in past 6h - skipping")
                continue
            
            # Send to Slack
            message = self._format_urgent_alert(market)
            success, slack_ts = self._send_to_slack(message)
            
            if success:
                # Log alert
                AlertLog.create(
                    market_id=market.market.id,
                    market_title=market.market.title,
                    tier=market.tier.value,
                    alert_type='real_time',
                    signals=market.signals,
                    slack_ts=slack_ts
                )
                logger.info(f"Sent urgent alert for: {market.market.title}")
            
            # Check if we've hit rate limit
            recent_count = AlertLog.get_recent_alert_count(hours=1)
            if recent_count >= self.max_alerts_per_hour:
                logger.warning("Rate limit reached - stopping alerts")
                break
    
    def queue_for_digest(self, hot_markets: List[HotMarket]) -> None:
        """Queue markets for scheduled digest"""
        for market in hot_markets:
            DigestQueue.add_to_queue(
                market_id=market.market.id,
                market_title=market.market.title,
                tier=market.tier.value,
                signals=market.signals,
                context=market.context
            )
            logger.info(f"Queued for digest: {market.market.title}")
    
    def send_digest(self, digest_type: str = 'morning') -> None:
        """Send scheduled digest"""
        
        # Get queued markets
        urgent_queued = DigestQueue.get_queued_markets(tier='urgent')
        daily_queued = DigestQueue.get_queued_markets(tier='daily')
        
        if not urgent_queued and not daily_queued:
            logger.info("No markets in digest queue")
            return
        
        # Format digest message
        message = self._format_digest(urgent_queued, daily_queued, digest_type)
        
        # Send to Slack
        success, _ = self._send_to_slack(message)
        
        if success:
            # Mark as sent
            all_market_ids = [m.market_id for m in urgent_queued + daily_queued]
            DigestQueue.mark_as_sent(all_market_ids)
            
            # Log digest
            for market in urgent_queued + daily_queued:
                AlertLog.create(
                    market_id=market.market_id,
                    market_title=market.market_title,
                    tier=market.alert_tier,
                    alert_type='digest',
                    signals=json.loads(market.signals)
                )
            
            logger.info(f"Sent {digest_type} digest with {len(all_market_ids)} markets")
    
    def _format_urgent_alert(self, hot_market: HotMarket) -> Dict:
        """Format urgent alert message for Slack"""
        market = hot_market.market
        context = hot_market.context
        
        # Build signal descriptions
        signal_descriptions = []
        for signal in hot_market.signals:
            if 'volume_spike' in signal:
                pct = context.get('volume_spike_1h', context.get('volume_spike_6h', 0)) * 100
                signal_descriptions.append(f"• Volume spike: +{pct:.0f}%")
            elif 'odds_swing' in signal or 'odds_movement' in signal:
                pts = context.get('odds_movement_1h', context.get('odds_movement_6h', 0)) * 100
                signal_descriptions.append(f"• Odds movement: {pts:.0f} points")
            elif 'new_high_profile' in signal:
                signal_descriptions.append(f"• New market with ${context.get('volume', 0):,.0f} volume")
            elif 'event_in' in signal:
                days = signal.split('_')[-2]
                signal_descriptions.append(f"• Event in {days} days")
        
        # Format odds
        odds_text = ""
        if market.current_odds:
            yes_pct = market.current_odds.get('yes', 0) * 100
            odds_text = f"{yes_pct:.0f}% Yes"
        
        # Build blocks (Slack Block Kit format)
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🔥 URGENT MARKET ALERT",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{market.title}*"
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Platform:*\n{market.platform.title()}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Volume:*\n${market.volume:,.0f}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Category:*\n{market.category or 'N/A'}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Odds:*\n{odds_text}"
                    }
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🎯 Why It's Hot:*\n" + "\n".join(signal_descriptions)
                }
            }
        ]
        
        # Add topic tier info
        if 'topic_tier' in context:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*📊 Topic Analysis:*\n• Tier: {context['topic_tier']}\n• {context.get('topic_reasoning', 'N/A')}"
                }
            })
        
        # Add event date if available
        if market.event_date:
            days_until = (market.event_date - datetime.now()).days
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*📅 Event Date:*\n{market.event_date.strftime('%B %d, %Y')} ({days_until} days)"
                }
            })
        
        blocks.append({"type": "divider"})
        
        return {
            "blocks": blocks,
            "text": f"🔥 URGENT: {market.title}"  # Fallback text
        }
    
    def _format_digest(self, urgent: List, daily: List, digest_type: str) -> Dict:
        """Format digest message for Slack"""
        
        title = "📊 MORNING DIGEST" if digest_type == 'morning' else "📊 EVENING DIGEST"
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{title} - {datetime.now().strftime('%B %d, %Y')}",
                    "emoji": True
                }
            }
        ]
        
        # Urgent section (missed overnight)
        if urgent:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🔥 URGENT ({len(urgent)} markets)*"
                }
            })
            
            for item in urgent[:5]:  # Top 5
                context = json.loads(item.context)
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"• *{item.market_title[:80]}...*\n  Volume: ${context.get('volume', 0):,.0f}"
                    }
                })
            
            blocks.append({"type": "divider"})
        
        # Daily opportunities
        if daily:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*📈 TODAY'S OPPORTUNITIES ({len(daily)} markets)*"
                }
            })
            
            for item in daily[:10]:  # Top 10
                context = json.loads(item.context)
                signals = json.loads(item.signals)
                
                # Create signal summary
                signal_summary = []
                for signal in signals:
                    if 'volume' in signal:
                        signal_summary.append("Volume growth")
                    elif 'event' in signal:
                        days = signal.split('_')[-2] if '_' in signal else '?'
                        signal_summary.append(f"Event in {days}d")
                    elif 'odds' in signal:
                        signal_summary.append("Odds movement")
                
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"• *{item.market_title[:80]}...*\n"
                            f"  {' • '.join(signal_summary[:2])}\n"
                            f"  Volume: ${context.get('volume', 0):,.0f}"
                        )
                    }
                })
        
        if not urgent and not daily:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "No hot markets detected in this period."
                }
            })
        
        return {
            "blocks": blocks,
            "text": f"{title} - {len(urgent) + len(daily)} hot markets"
        }
    
    def _send_to_slack(self, message: Dict) -> tuple[bool, Optional[str]]:
        """Send message to Slack via webhook"""
        
        if not self.slack_webhook_url:
            logger.warning("Slack webhook not configured - printing message instead:")
            print(json.dumps(message, indent=2))
            return False, None
        
        try:
            response = requests.post(
                self.slack_webhook_url,
                json=message,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                return True, None  # Webhooks don't return message ts
            else:
                logger.error(f"Slack API error: {response.status_code} - {response.text}")
                return False, None
                
        except Exception as e:
            logger.error(f"Error sending to Slack: {e}")
            return False, None
    
    def _is_business_hours(self) -> bool:
        """Check if current time is within business hours (9 AM - 6 PM)"""
        now = datetime.now()
        return 9 <= now.hour < 18