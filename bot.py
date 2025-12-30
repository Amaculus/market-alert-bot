"""
Prediction Market Alert Bot - Event-First Architecture

Monitors Kalshi and Polymarket for hot EVENTS (not just markets)
and sends alerts to Slack.
"""

import os
import time
import schedule
from datetime import datetime, timezone
from typing import List, Dict, Optional
import logging
import threading

from models import init_db, MarketSnapshot
from api_clients import MarketAggregator
from alerts import AlertManager, HotEvent, AlertTier
from relevance_checker import RelevanceChecker
from clustering import ClusteringEngine, MarketCluster

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MarketMonitor:
    """Event-First Market Monitor"""
    
    def __init__(self):
        logger.info("Initializing Event-First Market Monitor...")
        
        init_db()
        
        self.aggregator = MarketAggregator(
            kalshi_email=os.getenv('KALSHI_EMAIL'),
            kalshi_password=os.getenv('KALSHI_PASSWORD')
        )
        self.alert_manager = AlertManager()
        self.relevance_checker = RelevanceChecker()
        self.clustering = ClusteringEngine()
        
        self.run_lock = threading.Lock()
        
        # Config
        self.check_interval_minutes = int(os.getenv('CHECK_INTERVAL_MINUTES', '30'))
        
        # Volume thresholds
        self.min_event_volume_s = float(os.getenv('MIN_EVENT_VOLUME_S', '100000'))  # $100k for S-tier
        self.min_event_volume_a = float(os.getenv('MIN_EVENT_VOLUME_A', '250000'))  # $250k for A-tier
        self.min_event_volume_b = float(os.getenv('MIN_EVENT_VOLUME_B', '500000'))  # $500k for B-tier
        
        # Minimum volume to even consider
        self.absolute_min_volume = float(os.getenv('ABSOLUTE_MIN_VOLUME', '100000'))
        
        logger.info(f"Monitor initialized - checking every {self.check_interval_minutes} min")
        
    def check_markets(self) -> None:
        """Main check routine - Event-First approach"""
        
        if not self.run_lock.acquire(blocking=False):
            logger.warning("⚠️ SKIPPING CHECK: Previous check still running!")
            return

        try:
            logger.info("="*50)
            logger.info("Starting Event-First market check...")
            start_time = time.time()
            
            # 1. Fetch all markets
            all_markets = self.aggregator.fetch_all_markets()
            
            # 2. Filter by minimum volume
            viable_markets = [m for m in all_markets if m.volume >= self.absolute_min_volume]
            logger.info(f"STEP 1: {len(all_markets):,} → {len(viable_markets):,} viable markets")
            
            # 3. Event-First Clustering
            clusters = self.clustering.cluster_markets(viable_markets)
            logger.info(f"STEP 2: Grouped into {len(clusters):,} event clusters")
            
            # 4. Analyze Events (not individual markets)
            hot_events = []
            events_analyzed = 0
            
            for cluster in clusters:
                # Skip low-volume events entirely
                if cluster.total_volume < self.min_event_volume_s:
                    continue
                
                events_analyzed += 1
                
                # Analyze this event cluster
                hot_event = self._analyze_event(cluster)
                
                if hot_event:
                    hot_events.append(hot_event)
            
            logger.info(f"STEP 3: Analyzed {events_analyzed} events, found {len(hot_events)} hot")
            
            # 5. Process Alerts
            if hot_events:
                self._process_alerts(hot_events)
            else:
                logger.info("No hot events detected")
            
            elapsed = time.time() - start_time
            logger.info(f"Market check complete in {elapsed:.1f}s")
            logger.info("="*50)
            
        except Exception as e:
            logger.error(f"Error in market check: {e}", exc_info=True)
            
        finally:
            self.run_lock.release()
    
    def _analyze_event(self, cluster: MarketCluster) -> Optional[HotEvent]:
        """Analyze an event cluster for alertability"""
        
        # Check relevance using event title
        topic_info = self.relevance_checker.check_relevance(cluster.title)
        
        if not topic_info['is_relevant']:
            return None
        
        tier = topic_info['tier']
        
        # Check volume threshold for tier
        min_volume = self._get_min_volume_for_tier(tier)
        if cluster.total_volume < min_volume:
            return None
        
        # Get top markets in this cluster
        top_markets = cluster.get_top_markets(3)
        
        # Calculate signals across the cluster
        signals = self._calculate_cluster_signals(cluster)
        
        # Determine alert tier
        alert_tier = self._determine_alert_tier(cluster, signals, tier)
        
        if alert_tier == AlertTier.BACKGROUND:
            return None
        
        return HotEvent(
            cluster=cluster,
            tier=alert_tier,
            topic_tier=tier,
            signals=signals,
            top_markets=top_markets,
            context={
                'topic_tier': tier,
                'topic_reasoning': topic_info['reasoning'],
                'total_volume': cluster.total_volume,
                'market_count': cluster.market_count,
                'platforms': cluster.platform_spread,
                **signals
            }
        )
    
    def _calculate_cluster_signals(self, cluster: MarketCluster) -> Dict:
        """Calculate aggregate signals for the cluster"""
        signals = {
            'triggered': [],
            'volume_growth': 0,
            'top_mover_pct': 0,
            'has_odds_movement': False,
            'event_proximity_days': None,
        }
        
        # Check historical data for primary market
        primary = cluster.primary_market
        history = MarketSnapshot.get_history(primary.id, hours=6)
        
        if history:
            snapshot_1h = MarketSnapshot.get_snapshot_at(primary.id, hours=1)
            snapshot_6h = history[0] if history else None
            
            # Volume growth
            if snapshot_1h and snapshot_1h.volume > 0:
                growth = (cluster.total_volume - snapshot_1h.volume) / snapshot_1h.volume
                signals['volume_growth'] = growth
                
                if growth > 3.0:
                    signals['triggered'].append('volume_spike_300_1h')
                elif growth > 1.0:
                    signals['triggered'].append('volume_spike_100_1h')
            
            if snapshot_6h and snapshot_6h.volume > 0:
                growth_6h = (cluster.total_volume - snapshot_6h.volume) / snapshot_6h.volume
                if growth_6h > 2.0:
                    signals['triggered'].append('sustained_growth_6h')
        
        # Check for upcoming event
        if primary.event_date:
            try:
                now = datetime.now(timezone.utc)
                event_date = primary.event_date
                if event_date.tzinfo is None:
                    event_date = event_date.replace(tzinfo=timezone.utc)
                
                days_until = (event_date - now).days
                signals['event_proximity_days'] = days_until
                
                if 0 <= days_until <= 7:
                    signals['triggered'].append(f'event_in_{days_until}_days')
            except Exception:
                pass
        
        # High volume event (automatic trigger)
        if cluster.total_volume >= 500000:
            signals['triggered'].append('high_volume_event')
        
        # Multi-platform coverage
        if len(cluster.platform_spread) > 1:
            signals['triggered'].append('multi_platform')
        
        # Many markets = active event
        if cluster.market_count >= 5:
            signals['triggered'].append('active_event')
        
        return signals
    
    def _determine_alert_tier(self, cluster: MarketCluster, signals: Dict, topic_tier: str) -> AlertTier:
        """Determine alert tier based on signals"""
        
        triggered = signals.get('triggered', [])
        
        # URGENT: Major spike or S-tier with movement
        if 'volume_spike_300_1h' in triggered:
            return AlertTier.URGENT
        
        if topic_tier == 'S' and any(s in triggered for s in ['volume_spike_100_1h', 'event_in_0_days', 'event_in_1_days']):
            return AlertTier.URGENT
        
        # DAILY: Sustained interest or upcoming event
        if any(s in triggered for s in ['sustained_growth_6h', 'high_volume_event', 'multi_platform']):
            return AlertTier.DAILY
        
        if signals.get('event_proximity_days') is not None and signals['event_proximity_days'] <= 7:
            return AlertTier.DAILY
        
        if cluster.total_volume >= 250000 and topic_tier in ['S', 'A']:
            return AlertTier.DAILY
        
        return AlertTier.BACKGROUND
    
    def _get_min_volume_for_tier(self, tier: str) -> float:
        if tier == 'S':
            return self.min_event_volume_s
        elif tier == 'A':
            return self.min_event_volume_a
        else:
            return self.min_event_volume_b
    
    def _process_alerts(self, hot_events: List[HotEvent]) -> None:
        """Process and send alerts"""

        # Only create snapshots for URGENT events (reduce DB writes by ~80%)
        urgent = [e for e in hot_events if e.tier == AlertTier.URGENT]
        daily = [e for e in hot_events if e.tier == AlertTier.DAILY]

        # Snapshot only the primary market of urgent events
        for event in urgent:
            try:
                MarketSnapshot.create_from_market(event.cluster.primary_market)
            except Exception as e:
                logger.debug(f"Snapshot error: {e}")

        if urgent:
            self.alert_manager.send_urgent_event_alerts(urgent)

        if daily:
            self.alert_manager.queue_events_for_digest(daily)
    
    def send_morning_digest(self) -> None:
        try:
            logger.info("Sending morning digest...")
            self.alert_manager.send_digest(digest_type='morning')
        except Exception as e:
            logger.error(f"Error sending morning digest: {e}", exc_info=True)
    
    def send_evening_digest(self) -> None:
        try:
            logger.info("Sending evening digest...")
            self.alert_manager.send_digest(digest_type='evening')
        except Exception as e:
            logger.error(f"Error sending evening digest: {e}", exc_info=True)

    def cleanup_database(self) -> None:
        """Daily database cleanup to prevent bloat"""
        try:
            logger.info("Running database cleanup...")

            # Delete old snapshots (keep 7 days)
            snapshot_deleted = MarketSnapshot.cleanup_old_snapshots(days=7)
            logger.info(f"Deleted {snapshot_deleted} old snapshots")

            # Delete old alert logs (keep 30 days)
            from models import AlertLog, DigestQueue
            alerts_deleted = AlertLog.cleanup_old_logs(days=30)
            logger.info(f"Deleted {alerts_deleted} old alert logs")

            # Delete sent digest items (keep 7 days)
            digest_deleted = DigestQueue.cleanup_old_sent_items(days=7)
            logger.info(f"Deleted {digest_deleted} old digest items")

            logger.info("Database cleanup complete")
        except Exception as e:
            logger.error(f"Error during database cleanup: {e}", exc_info=True)


def main():
    logger.info("="*60)
    logger.info("PREDICTION MARKET ALERT BOT (Event-First)")
    logger.info("="*60)
    logger.info(f"Environment: {os.getenv('RAILWAY_ENVIRONMENT', 'development')}")
    logger.info(f"Slack configured: {bool(os.getenv('SLACK_WEBHOOK_URL'))}")
    
    monitor = MarketMonitor()
    
    check_interval = monitor.check_interval_minutes
    
    logger.info(f"Scheduling checks every {check_interval} minutes")
    schedule.every(check_interval).minutes.do(monitor.check_markets)
    
    morning_time = os.getenv('MORNING_DIGEST_TIME', '09:00')
    evening_time = os.getenv('EVENING_DIGEST_TIME', '17:00')
    cleanup_time = os.getenv('CLEANUP_TIME', '03:00')

    logger.info(f"Morning digest at {morning_time}")
    schedule.every().day.at(morning_time).do(monitor.send_morning_digest)

    logger.info(f"Evening digest at {evening_time}")
    schedule.every().day.at(evening_time).do(monitor.send_evening_digest)

    logger.info(f"Database cleanup at {cleanup_time}")
    schedule.every().day.at(cleanup_time).do(monitor.cleanup_database)
    
    # Initial check in background
    logger.info("Running initial check...")
    initial_thread = threading.Thread(target=monitor.check_markets)
    initial_thread.start()
    
    logger.info("Bot running. Scheduler active.")
    logger.info("="*60)
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            break
        except Exception as e:
            logger.error(f"Main loop error: {e}", exc_info=True)
            time.sleep(60)


if __name__ == "__main__":
    main()