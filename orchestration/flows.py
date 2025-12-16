"""
Prefect Orchestration
=====================
Data pipeline flows for scheduled scraping and processing.
"""

import asyncio
from datetime import datetime, timedelta
from typing import List, Dict

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.reddit import RedditScraper, RedditPost
from scrapers.hackernews import HackerNewsScraper, HNStory
from nlp.pipeline import NLPPipeline, NLPResult
from database.models import db, Post


@task(retries=3, retry_delay_seconds=60)
async def scrape_reddit_task(limit_per_sub: int = 50) -> List[Dict]:
    """Scrape Reddit posts."""
    logger = get_run_logger()
    logger.info(f"Scraping Reddit (limit: {limit_per_sub}/sub)...")
    
    async with RedditScraper() as scraper:
        posts = await scraper.scrape_all(limit_per_sub=limit_per_sub)
    
    logger.info(f"Scraped {len(posts)} Reddit posts")
    return [p.to_dict() for p in posts]


@task(retries=3, retry_delay_seconds=60)
async def scrape_hackernews_task(limit_per_type: int = 50) -> List[Dict]:
    """Scrape HackerNews stories."""
    logger = get_run_logger()
    logger.info(f"Scraping HackerNews (limit: {limit_per_type}/type)...")
    
    async with HackerNewsScraper() as scraper:
        stories = await scraper.scrape_all(limit_per_type=limit_per_type)
    
    logger.info(f"Scraped {len(stories)} HN stories")
    return [s.to_dict() for s in stories]


@task
def analyze_nlp_task(posts: List[Dict], source: str) -> List[Dict]:
    """Run NLP analysis on posts."""
    logger = get_run_logger()
    logger.info(f"Analyzing {len(posts)} {source} posts with NLP...")
    
    pipeline = NLPPipeline()
    
    enriched = []
    for post in posts:
        # Calculate age
        created = post.get("created_utc", datetime.now().isoformat())
        if isinstance(created, str):
            try:
                created_dt = datetime.fromisoformat(created)
            except:
                created_dt = datetime.now()
        else:
            created_dt = created
        
        age_hours = (datetime.now() - created_dt).total_seconds() / 3600
        
        # Run NLP
        result = pipeline.analyze(
            title=post.get("title", ""),
            body=post.get("body", post.get("text", "")),
            score=post.get("score", 0),
            num_comments=post.get("num_comments", 0),
            age_hours=max(age_hours, 0.1)
        )
        
        # Merge results
        enriched_post = {
            "external_id": f"{source}_{post.get('id', '')}",
            "source": source,
            "title": post.get("title", ""),
            "body": post.get("body", post.get("text", "")),
            "url": post.get("url", ""),
            "author": post.get("author", ""),
            "score": post.get("score", 0),
            "num_comments": post.get("num_comments", 0),
            "upvote_ratio": post.get("upvote_ratio"),
            "subreddit": post.get("subreddit"),
            "story_type": post.get("story_type"),
            "created_at": created_dt,
            "scraped_at": datetime.now(),
            "sentiment_score": result.sentiment_score,
            "sentiment_label": result.sentiment_label,
            "topics": result.topics,
            "keywords": result.keywords,
            "entities": [e for e in result.entities],
            "viral_score": result.viral_score,
            "engagement_prediction": result.engagement_prediction
        }
        enriched.append(enriched_post)
    
    logger.info(f"NLP analysis complete for {source}")
    return enriched


@task
def store_posts_task(posts: List[Dict]) -> int:
    """Store posts in database."""
    logger = get_run_logger()
    logger.info(f"Storing {len(posts)} posts...")
    
    count = db.insert_posts_batch(posts)
    logger.info(f"Stored {count} new posts (skipped {len(posts) - count} duplicates)")
    return count


@task
def generate_snapshot_task() -> Dict:
    """Generate trend snapshot."""
    logger = get_run_logger()
    logger.info("Generating trend snapshot...")
    
    with db.session() as session:
        # Get trending topics
        topics = db.get_trending_topics(session, hours=24)
        
        # Get recent posts for stats
        posts = db.get_recent_posts(session, hours=24, limit=1000)
        
        snapshot = {
            "timestamp": datetime.now().isoformat(),
            "total_posts_24h": len(posts),
            "trending_topics": topics[:10],
            "sentiment_breakdown": {
                "positive": len([p for p in posts if p.sentiment_label == "positive"]),
                "neutral": len([p for p in posts if p.sentiment_label == "neutral"]),
                "negative": len([p for p in posts if p.sentiment_label == "negative"])
            },
            "top_viral": [
                {"title": p.title[:100], "score": p.viral_score, "source": p.source}
                for p in sorted(posts, key=lambda x: x.viral_score or 0, reverse=True)[:5]
            ]
        }
    
    logger.info(f"Snapshot generated: {snapshot['total_posts_24h']} posts, {len(topics)} topics")
    return snapshot


@flow(name="scrape-all-sources")
async def scrape_all_flow(reddit_limit: int = 50, hn_limit: int = 50):
    """Main scraping flow - runs all sources."""
    logger = get_run_logger()
    logger.info("Starting full scrape flow...")
    
    # Ensure tables exist
    db.create_tables()
    
    # Scrape in parallel
    reddit_posts, hn_posts = await asyncio.gather(
        scrape_reddit_task(reddit_limit),
        scrape_hackernews_task(hn_limit)
    )
    
    # Process with NLP
    reddit_enriched = analyze_nlp_task(reddit_posts, "reddit")
    hn_enriched = analyze_nlp_task(hn_posts, "hackernews")
    
    # Store
    reddit_count = store_posts_task(reddit_enriched)
    hn_count = store_posts_task(hn_enriched)
    
    # Generate snapshot
    snapshot = generate_snapshot_task()
    
    logger.info(f"Flow complete! Reddit: {reddit_count}, HN: {hn_count}")
    return {
        "reddit_stored": reddit_count,
        "hn_stored": hn_count,
        "snapshot": snapshot
    }


@flow(name="quick-scrape")
async def quick_scrape_flow():
    """Quick scrape for testing."""
    return await scrape_all_flow(reddit_limit=10, hn_limit=10)


@flow(name="hourly-scrape")
async def hourly_scrape_flow():
    """Hourly scrape - moderate volume."""
    return await scrape_all_flow(reddit_limit=25, hn_limit=25)


@flow(name="daily-scrape")
async def daily_scrape_flow():
    """Daily deep scrape - high volume."""
    return await scrape_all_flow(reddit_limit=100, hn_limit=100)


def run_scrape():
    """Run scrape flow (for CLI)."""
    asyncio.run(scrape_all_flow())


if __name__ == "__main__":
    # Test run
    asyncio.run(quick_scrape_flow())
