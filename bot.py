"""
Prediction Market Alert Bot - Main Worker

Continuously monitors Kalshi and Polymarket for hot markets
and sends alerts to Slack.
"""

import os
import time
import schedule
from datetime import datetime, timedelta, timezone
from typing import List, Dict
import logging

from models import init_db, MarketSnapshot, AlertLog
from api_clients import MarketAggregator
from alerts import AlertManager, HotMarket, AlertTier
from relevance_checker import RelevanceChecker

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MarketMonitor:
    def __init__(self):
        logger.info("Initializing Market Monitor...")
        init_db()
        
        self.aggregator = MarketAggregator(
            kalshi_email=os.getenv('KALSHI_EMAIL'),
            kalshi_password=os.getenv('KALSHI_PASSWORD')
        )
        self.alert_manager = AlertManager()
        self.relevance_checker = RelevanceChecker()
        self.clustering = ClusteringEngine() # NEW
        
        self.check_interval_minutes = int(os.getenv('CHECK_INTERVAL_MINUTES', '30'))
        
        # INCREASED THRESHOLDS for high-volume environment
        self.min_volume_tier_s = float(os.getenv('MIN_VOLUME_TIER_S', '50000'))
        self.min_volume_tier_a = float(os.getenv('MIN_VOLUME_TIER_A', '100000'))
        self.min_volume_tier_c = float(os.getenv('MIN_VOLUME_TIER_C', '250000'))
        
        # ABSOLUTE FLOOR: Ignore anything below this before DB/Clustering
        self.absolute_min_volume = 1000.0 

    def check_markets(self) -> None:
        try:
            logger.info("Starting market check...")
            start_time = time.time()
            
            # 1. Fetch
            all_markets = self.aggregator.fetch_all_markets()
            
            # 2. JUNK FILTER: Drop tiny markets immediately
            # This saves DB space and CPU time
            viable_markets = [m for m in all_markets if m.volume > self.absolute_min_volume]
            logger.info(f"Filtered {len(all_markets)} -> {len(viable_markets)} viable markets (>{self.absolute_min_volume} vol)")
            
            # 3. CLUSTER: Group duplicates
            clusters = self.clustering.cluster_markets(viable_markets)
            logger.info(f"Grouped into {len(clusters)} unique topics")
            
            hot_markets = []
            
            for cluster in clusters:
                # Use TOTAL cluster volume for the initial threshold check
                # This catches split liquidity (e.g. $30k on Kalshi + $30k on Poly = $60k Total)
                if cluster.total_volume < self.min_volume_tier_s:
                    continue

                # We analyze the primary market, but temporarily inject total volume
                market = cluster.primary_market
                original_vol = market.volume
                market.volume = cluster.total_volume 
                
                # Save/Analyze
                snapshot = MarketSnapshot.create_from_market(market)
                hot_market = self._analyze_market(market, snapshot)
                
                market.volume = original_vol # Reset
                
                if hot_market:
                    hot_markets.append(hot_market)
            
            if hot_markets:
                self._process_alerts(hot_markets)
            
            logger.info(f"Market check complete in {time.time() - start_time:.2f}s")
            
        except Exception as e:
            logger.error(f"Error in market check: {e}", exc_info=True)

    def _analyze_market(self, market, current_snapshot) -> HotMarket:
        # (Keep your existing _analyze_market logic exactly as is)
        history = MarketSnapshot.get_history(market.id, hours=6)
        if not history:
            return self._check_new_market(market, current_snapshot)
            
        snapshot_1h = MarketSnapshot.get_snapshot_at(market.id, hours=1)
        snapshot_6h = history[0] if history else None
        
        signals = {}
        # ... (Same signal calculation logic as your file) ...
        # Volume spike detection
        if snapshot_1h and snapshot_1h.volume > 0:
            signals['volume_spike_1h'] = (current_snapshot.volume - snapshot_1h.volume) / snapshot_1h.volume
        if snapshot_6h and snapshot_6h.volume > 0:
            signals['volume_spike_6h'] = (current_snapshot.volume - snapshot_6h.volume) / snapshot_6h.volume
        # Odds detection
        if snapshot_1h and snapshot_1h.yes_odds:
            signals['odds_movement_1h'] = abs(current_snapshot.yes_odds - snapshot_1h.yes_odds)
        if snapshot_6h and snapshot_6h.yes_odds:
            signals['odds_movement_6h'] = abs(current_snapshot.yes_odds - snapshot_6h.yes_odds)
        # Event proximity
        if market.event_date:
            days_until = (market.event_date - datetime.now(timezone.utc)).days
            if 3 <= days_until <= 7:
                signals['event_proximity'] = days_until

        return self._evaluate_signals(market, current_snapshot, signals)

    def _check_new_market(self, market, snapshot) -> HotMarket:
        # Keep existing logic
        age_hours = (datetime.now() - snapshot.created_at).total_seconds() / 3600
        if age_hours > 24: return None
        if snapshot.volume < 50000: return None # High bar for new markets
        
        topic_info = self.relevance_checker.check_relevance(market.title)
        if not topic_info['is_relevant']: return None
        
        min_volume = self._get_min_volume_for_tier(topic_info['tier'])
        if snapshot.volume < min_volume: return None
        
        return HotMarket(
            market=market, tier=AlertTier.URGENT, signals=['new_high_profile_market'],
            context={'reason': 'New high-profile', 'topic_tier': topic_info['tier'], 'volume': snapshot.volume}
        )

    def _evaluate_signals(self, market, snapshot, signals: Dict) -> HotMarket:
        # Keep existing logic
        lowest_threshold = min(self.min_volume_tier_s, self.min_volume_tier_a, self.min_volume_tier_c)
        if snapshot.volume < lowest_threshold: return None

        topic_info = self.relevance_checker.check_relevance(market.title)
        if not topic_info['is_relevant']: return None
        
        min_volume = self._get_min_volume_for_tier(topic_info['tier'])
        if snapshot.volume < min_volume: return None
        
        triggered_signals = []
        tier = AlertTier.BACKGROUND
        
        # (Paste your existing signal logic here - Tier 1, Tier 2 checks)
        # Tier 1: URGENT
        if signals.get('volume_spike_1h', 0) > 3.0:
            triggered_signals.append('volume_spike_300_1h')
            tier = AlertTier.URGENT
        if topic_info['tier'] == 'S' and signals.get('odds_movement_1h', 0) > 0.20:
            triggered_signals.append('odds_swing_20_1h')
            tier = AlertTier.URGENT
            
        # Tier 2: DAILY
        if signals.get('volume_spike_6h', 0) > 2.0 and tier != AlertTier.URGENT:
            triggered_signals.append('sustained_volume_growth')
            tier = AlertTier.DAILY
        if signals.get('event_proximity') and tier == AlertTier.BACKGROUND:
            triggered_signals.append(f"event_in_{signals['event_proximity']}_days")
            tier = AlertTier.DAILY
        if signals.get('odds_movement_6h', 0) > 0.15 and tier == AlertTier.BACKGROUND:
            triggered_signals.append('odds_movement_15_6h')
            tier = AlertTier.DAILY
            
        if not triggered_signals: return None
        
        return HotMarket(
            market=market, tier=tier, signals=triggered_signals,
            context={'topic_tier': topic_info['tier'], 'volume': snapshot.volume, **signals}
        )

    def _get_min_volume_for_tier(self, tier: str) -> float:
        if tier == 'S': return self.min_volume_tier_s
        elif tier == 'A': return self.min_volume_tier_a
        else: return self.min_volume_tier_c
        
    def _process_alerts(self, hot_markets):
        urgent = [m for m in hot_markets if m.tier == AlertTier.URGENT]
        daily = [m for m in hot_markets if m.tier == AlertTier.DAILY]
        if urgent: self.alert_manager.send_urgent_alerts(urgent)
        if daily: self.alert_manager.queue_for_digest(daily)
    
    def send_morning_digest(self) -> None:
        """Send morning digest at 9 AM"""
        try:
            logger.info("Sending morning digest...")
            self.alert_manager.send_digest(digest_type='morning')
        except Exception as e:
            logger.error(f"Error sending morning digest: {e}", exc_info=True)
    
    def send_evening_digest(self) -> None:
        """Send evening digest at 5 PM"""
        try:
            logger.info("Sending evening digest...")
            self.alert_manager.send_digest(digest_type='evening')
        except Exception as e:
            logger.error(f"Error sending evening digest: {e}", exc_info=True)


def main():
    """Main entry point - runs the bot"""
    logger.info("="*60)
    logger.info("PREDICTION MARKET ALERT BOT")
    logger.info("="*60)
    logger.info(f"Environment: {os.getenv('RAILWAY_ENVIRONMENT', 'development')}")
    logger.info(f"Slack webhook configured: {bool(os.getenv('SLACK_WEBHOOK_URL'))}")
    
    # Initialize monitor
    monitor = MarketMonitor()
    
    # Schedule tasks
    check_interval = monitor.check_interval_minutes
    logger.info(f"Scheduling market checks every {check_interval} minutes")
    schedule.every(check_interval).minutes.do(monitor.check_markets)
    
    # Schedule digests (9 AM and 5 PM)
    morning_time = os.getenv('MORNING_DIGEST_TIME', '09:00')
    evening_time = os.getenv('EVENING_DIGEST_TIME', '17:00')
    
    logger.info(f"Scheduling morning digest at {morning_time}")
    schedule.every().day.at(morning_time).do(monitor.send_morning_digest)
    
    logger.info(f"Scheduling evening digest at {evening_time}")
    schedule.every().day.at(evening_time).do(monitor.send_evening_digest)
    
    # Run initial check
    logger.info("Running initial market check...")
    monitor.check_markets()
    
    # Main loop
    logger.info("Bot is now running. Press Ctrl+C to stop.")
    logger.info("="*60)
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every minute for scheduled tasks
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            break
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
            time.sleep(60)


if __name__ == "__main__":
    main()