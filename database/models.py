"""
Database Layer
==============
PostgreSQL connection and ORM models.
"""

import os
from datetime import datetime
from typing import List, Optional, Dict
from contextlib import contextmanager

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, JSON, Boolean, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/social_pulse")

# For SQLite fallback (easier local dev)
SQLITE_URL = "sqlite:///social_pulse.db"

Base = declarative_base()


class Post(Base):
    """Social media post model."""
    __tablename__ = "posts"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    external_id = Column(String(100), unique=True, index=True)
    source = Column(String(50), index=True)  # reddit, hackernews, twitter
    
    # Content
    title = Column(Text)
    body = Column(Text)
    url = Column(Text)
    author = Column(String(200))
    
    # Metrics
    score = Column(Integer, default=0)
    num_comments = Column(Integer, default=0)
    upvote_ratio = Column(Float)
    
    # Metadata
    subreddit = Column(String(100), index=True)  # Reddit only
    story_type = Column(String(50))  # HN only
    
    # Timestamps
    created_at = Column(DateTime)
    scraped_at = Column(DateTime, default=datetime.now)
    
    # NLP Results
    sentiment_score = Column(Float)
    sentiment_label = Column(String(20))
    topics = Column(JSON)
    keywords = Column(JSON)
    entities = Column(JSON)
    viral_score = Column(Float)
    engagement_prediction = Column(String(20))
    
    __table_args__ = (
        Index('idx_source_created', 'source', 'created_at'),
        Index('idx_sentiment', 'sentiment_label'),
        Index('idx_viral', 'viral_score'),
    )


class TrendSnapshot(Base):
    """Hourly trend snapshot."""
    __tablename__ = "trend_snapshots"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_time = Column(DateTime, index=True)
    
    # Aggregations
    source = Column(String(50))
    topic = Column(String(100), index=True)
    post_count = Column(Integer)
    avg_score = Column(Float)
    avg_sentiment = Column(Float)
    avg_viral_score = Column(Float)
    top_keywords = Column(JSON)
    
    __table_args__ = (
        Index('idx_snapshot_topic', 'snapshot_time', 'topic'),
    )


class TopEntity(Base):
    """Trending entities over time."""
    __tablename__ = "top_entities"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_time = Column(DateTime, index=True)
    entity_text = Column(String(200))
    entity_type = Column(String(50))
    mention_count = Column(Integer)
    avg_sentiment = Column(Float)
    sources = Column(JSON)


class Database:
    """Database connection manager."""
    
    def __init__(self, use_sqlite: bool = True):
        if use_sqlite:
            self.url = SQLITE_URL
            self.engine = create_engine(SQLITE_URL, echo=False)
        else:
            self.url = DATABASE_URL
            self.engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
        
        self.SessionLocal = sessionmaker(bind=self.engine)
    
    def create_tables(self):
        """Create all tables."""
        Base.metadata.create_all(self.engine)
        print(f"✅ Database tables created at {self.url}")
    
    def drop_tables(self):
        """Drop all tables."""
        Base.metadata.drop_all(self.engine)
    
    @contextmanager
    def session(self):
        """Get database session."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def insert_post(self, session: Session, post_data: Dict) -> Optional[Post]:
        """Insert a post if not exists."""
        existing = session.query(Post).filter_by(external_id=post_data['external_id']).first()
        if existing:
            return None
        
        post = Post(**post_data)
        session.add(post)
        return post
    
    def insert_posts_batch(self, posts_data: List[Dict]) -> int:
        """Insert multiple posts, skip duplicates."""
        count = 0
        with self.session() as session:
            for data in posts_data:
                if self.insert_post(session, data):
                    count += 1
        return count
    
    def get_recent_posts(self, session: Session, source: str = None, 
                         hours: int = 24, limit: int = 100) -> List[Post]:
        """Get recent posts."""
        from datetime import timedelta
        cutoff = datetime.now() - timedelta(hours=hours)
        
        query = session.query(Post).filter(Post.scraped_at >= cutoff)
        if source:
            query = query.filter(Post.source == source)
        
        return query.order_by(Post.score.desc()).limit(limit).all()
    
    def get_trending_topics(self, session: Session, hours: int = 24) -> List[Dict]:
        """Get trending topics in last N hours."""
        from datetime import timedelta
        from sqlalchemy import func
        
        cutoff = datetime.now() - timedelta(hours=hours)
        
        # Get all posts with topics
        posts = session.query(Post).filter(
            Post.scraped_at >= cutoff,
            Post.topics.isnot(None)
        ).all()
        
        # Aggregate topics
        topic_stats = {}
        for post in posts:
            for topic in (post.topics or []):
                if topic not in topic_stats:
                    topic_stats[topic] = {
                        "topic": topic,
                        "count": 0,
                        "total_score": 0,
                        "total_sentiment": 0
                    }
                topic_stats[topic]["count"] += 1
                topic_stats[topic]["total_score"] += post.score or 0
                topic_stats[topic]["total_sentiment"] += post.sentiment_score or 0
        
        # Calculate averages
        results = []
        for topic, stats in topic_stats.items():
            results.append({
                "topic": topic,
                "post_count": stats["count"],
                "avg_score": stats["total_score"] / stats["count"],
                "avg_sentiment": stats["total_sentiment"] / stats["count"]
            })
        
        return sorted(results, key=lambda x: x["post_count"], reverse=True)
    
    def get_sentiment_over_time(self, session: Session, topic: str = None, 
                                 hours: int = 24) -> List[Dict]:
        """Get sentiment trends over time."""
        from datetime import timedelta
        from sqlalchemy import func
        
        cutoff = datetime.now() - timedelta(hours=hours)
        
        query = session.query(
            func.strftime('%Y-%m-%d %H:00', Post.scraped_at).label('hour'),
            func.avg(Post.sentiment_score).label('avg_sentiment'),
            func.count(Post.id).label('count')
        ).filter(Post.scraped_at >= cutoff)
        
        if topic:
            # SQLite JSON handling
            query = query.filter(Post.topics.like(f'%{topic}%'))
        
        results = query.group_by('hour').order_by('hour').all()
        
        return [{"hour": r.hour, "avg_sentiment": r.avg_sentiment, "count": r.count} for r in results]


# Global instance
db = Database(use_sqlite=True)


def main():
    """Test database."""
    print("Testing Database...")
    
    db.create_tables()
    
    # Test insert
    test_data = {
        "external_id": "test_123",
        "source": "reddit",
        "title": "Test post about AI",
        "body": "This is a test post",
        "author": "test_user",
        "score": 100,
        "num_comments": 50,
        "created_at": datetime.now(),
        "sentiment_score": 0.5,
        "sentiment_label": "positive",
        "topics": ["AI/ML", "Tech Industry"],
        "keywords": ["ai", "test", "post"],
        "viral_score": 0.6,
        "engagement_prediction": "medium"
    }
    
    count = db.insert_posts_batch([test_data])
    print(f"Inserted {count} posts")
    
    with db.session() as session:
        posts = db.get_recent_posts(session, limit=5)
        print(f"Recent posts: {len(posts)}")
        for post in posts:
            print(f"  [{post.source}] {post.title[:50]}...")


if __name__ == "__main__":
    main()
