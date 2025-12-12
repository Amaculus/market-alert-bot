"""
Database Models

Stores market snapshots and alert logs using SQLAlchemy.
"""

import os
from datetime import datetime, timedelta
from typing import List, Optional
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

# Database setup
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")

# Railway Postgres URLs start with postgres://, SQLAlchemy needs postgresql://
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()


class MarketSnapshot(Base):
    """Stores market data at each check interval"""
    
    __tablename__ = 'market_snapshots'
    
    id = Column(Integer, primary_key=True)
    market_id = Column(String(255), nullable=False, index=True)
    platform = Column(String(50), nullable=False)
    title = Column(Text, nullable=False)
    category = Column(String(100))
    series_ticker = Column(String(100))
    
    # Market data
    volume = Column(Float, default=0.0)
    yes_odds = Column(Float)
    no_odds = Column(Float)
    trader_count = Column(Integer)
    
    # Event info
    event_date = Column(DateTime)
    
    # Timestamp
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    @classmethod
    def create_from_market(cls, market):
        """Create snapshot from Market object"""
        from market import Market
        
        session = SessionLocal()
        try:
            snapshot = cls(
                market_id=market.id,
                platform=market.platform,
                title=market.title,
                category=market.category,
                series_ticker=market.series_ticker,
                volume=market.volume,
                yes_odds=market.current_odds.get('yes') if market.current_odds else None,
                no_odds=market.current_odds.get('no') if market.current_odds else None,
                event_date=market.event_date,
                created_at=datetime.utcnow()
            )
            
            session.add(snapshot)
            session.commit()
            
            # --- ADD THIS LINE ---
            session.refresh(snapshot) 
            # ---------------------
            
            return snapshot
            
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()


    @classmethod
    def get_history(cls, market_id: str, hours: int = 6) -> List['MarketSnapshot']:
        """Get historical snapshots for a market"""
        session = SessionLocal()
        try:
            cutoff = datetime.utcnow() - timedelta(hours=hours)
            snapshots = session.query(cls)\
                .filter(cls.market_id == market_id)\
                .filter(cls.created_at >= cutoff)\
                .order_by(cls.created_at.asc())\
                .all()
            return snapshots
        finally:
            session.close()
    
    @classmethod
    def get_snapshot_at(cls, market_id: str, hours: int = 1) -> Optional['MarketSnapshot']:
        """Get snapshot from approximately N hours ago"""
        session = SessionLocal()
        try:
            target_time = datetime.utcnow() - timedelta(hours=hours)
            
            # Get closest snapshot to target time
            snapshot = session.query(cls)\
                .filter(cls.market_id == market_id)\
                .filter(cls.created_at <= target_time)\
                .order_by(cls.created_at.desc())\
                .first()
            
            return snapshot
        finally:
            session.close()
    
    @classmethod
    def get_latest(cls, market_id: str) -> Optional['MarketSnapshot']:
        """Get most recent snapshot for a market"""
        session = SessionLocal()
        try:
            snapshot = session.query(cls)\
                .filter(cls.market_id == market_id)\
                .order_by(cls.created_at.desc())\
                .first()
            return snapshot
        finally:
            session.close()


class AlertLog(Base):
    """Logs all alerts sent"""
    
    __tablename__ = 'alert_logs'
    
    id = Column(Integer, primary_key=True)
    market_id = Column(String(255), nullable=False, index=True)
    market_title = Column(Text, nullable=False)
    alert_tier = Column(String(50), nullable=False)
    alert_type = Column(String(50), nullable=False)  # 'real_time' or 'digest'
    signals = Column(Text)  # JSON string of triggered signals
    
    # Alert tracking
    sent_at = Column(DateTime, default=datetime.utcnow, index=True)
    slack_message_ts = Column(String(50))  # Slack message timestamp for threading
    
    @classmethod
    def create(cls, market_id: str, market_title: str, tier: str, 
               alert_type: str, signals: List[str], slack_ts: str = None):
        """Create alert log entry"""
        import json
        
        session = SessionLocal()
        try:
            log = cls(
                market_id=market_id,
                market_title=market_title,
                alert_tier=tier,
                alert_type=alert_type,
                signals=json.dumps(signals),
                slack_message_ts=slack_ts
            )
            session.add(log)
            session.commit()
            return log
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    @classmethod
    def was_alerted_recently(cls, market_id: str, hours: int = 6) -> bool:
        """Check if market was already alerted in past N hours"""
        session = SessionLocal()
        try:
            cutoff = datetime.utcnow() - timedelta(hours=hours)
            exists = session.query(cls)\
                .filter(cls.market_id == market_id)\
                .filter(cls.sent_at >= cutoff)\
                .filter(cls.alert_type == 'real_time')\
                .first()
            return exists is not None
        finally:
            session.close()
    
    @classmethod
    def get_recent_alert_count(cls, hours: int = 1) -> int:
        """Get count of alerts sent in past N hours"""
        session = SessionLocal()
        try:
            cutoff = datetime.utcnow() - timedelta(hours=hours)
            count = session.query(cls)\
                .filter(cls.sent_at >= cutoff)\
                .filter(cls.alert_type == 'real_time')\
                .count()
            return count
        finally:
            session.close()


class DigestQueue(Base):
    """Queues markets for inclusion in scheduled digests"""
    
    __tablename__ = 'digest_queue'
    
    id = Column(Integer, primary_key=True)
    market_id = Column(String(255), nullable=False)
    market_title = Column(Text, nullable=False)
    alert_tier = Column(String(50), nullable=False)
    signals = Column(Text)  # JSON string
    context = Column(Text)  # JSON string
    
    # Queue info
    queued_at = Column(DateTime, default=datetime.utcnow, index=True)
    included_in_digest = Column(Boolean, default=False)
    digest_sent_at = Column(DateTime)
    
    @classmethod
    def add_to_queue(cls, market_id: str, market_title: str, tier: str,
                     signals: List[str], context: dict):
        """Add market to digest queue"""
        import json
        
        session = SessionLocal()
        try:
            # Check if already in queue
            existing = session.query(cls)\
                .filter(cls.market_id == market_id)\
                .filter(cls.included_in_digest == False)\
                .first()
            
            if existing:
                # Update existing entry
                existing.signals = json.dumps(signals)
                existing.context = json.dumps(context)
                existing.queued_at = datetime.utcnow()
            else:
                # Create new entry
                item = cls(
                    market_id=market_id,
                    market_title=market_title,
                    alert_tier=tier,
                    signals=json.dumps(signals),
                    context=json.dumps(context)
                )
                session.add(item)
            
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    @classmethod
    def get_queued_markets(cls, tier: str = None) -> List['DigestQueue']:
        """Get markets waiting in digest queue"""
        session = SessionLocal()
        try:
            query = session.query(cls)\
                .filter(cls.included_in_digest == False)
            
            if tier:
                query = query.filter(cls.alert_tier == tier)
            
            return query.order_by(cls.queued_at.desc()).all()
        finally:
            session.close()
    
    @classmethod
    def mark_as_sent(cls, market_ids: List[str]):
        """Mark markets as included in digest"""
        session = SessionLocal()
        try:
            session.query(cls)\
                .filter(cls.market_id.in_(market_ids))\
                .update({
                    'included_in_digest': True,
                    'digest_sent_at': datetime.utcnow()
                }, synchronize_session=False)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()


def init_db():
    """Initialize database - create all tables"""
    Base.metadata.create_all(bind=engine)
    print("Database initialized")


if __name__ == "__main__":
    init_db()
    print("Database tables created successfully")

class TopicCache(Base):
    """Caches AI relevance checks to save API costs"""
    __tablename__ = 'topic_cache'
    
    topic_hash = Column(String(255), primary_key=True) # Use title as key
    is_relevant = Column(Boolean)
    tier = Column(String(10))
    reasoning = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

    @classmethod
    def get(cls, title: str):
        session = SessionLocal()
        try:
            return session.query(cls).filter(cls.topic_hash == title).first()
        finally:
            session.close()

    @classmethod
    def set(cls, title: str, data: dict):
        session = SessionLocal()
        try:
            cache = cls(
                topic_hash=title,
                is_relevant=data['is_relevant'],
                tier=data['tier'],
                reasoning=data['reasoning']
            )
            session.merge(cache) # Update if exists
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Cache error: {e}")
        finally:
            session.close()