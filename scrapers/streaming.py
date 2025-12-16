"""
Streaming Scraper
=================
Continuously scrapes Reddit and HackerNews, publishes to Kafka.
Runs as a daemon with configurable intervals.
"""

import asyncio
import os
import signal
import sys
from datetime import datetime
from typing import List, Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.reddit import RedditScraper
from scrapers.hackernews import HackerNewsScraper
from kafka.producer import get_producer


class StreamingScraper:
    """Continuous scraper that publishes to Kafka."""
    
    def __init__(
        self,
        reddit_interval: int = 300,    # 5 minutes
        hn_interval: int = 180,        # 3 minutes
        reddit_limit: int = 50,
        hn_limit: int = 50
    ):
        self.reddit_interval = reddit_interval
        self.hn_interval = hn_interval
        self.reddit_limit = reddit_limit
        self.hn_limit = hn_limit
        
        self.producer = get_producer()
        self.running = True
        
        # Stats
        self.stats = {
            'reddit_scrapes': 0,
            'reddit_posts': 0,
            'hn_scrapes': 0,
            'hn_posts': 0,
            'started_at': datetime.now().isoformat(),
            'last_reddit': None,
            'last_hn': None
        }
    
    async def scrape_reddit_loop(self):
        """Continuously scrape Reddit."""
        print(f"🔴 Reddit scraper started (every {self.reddit_interval}s)")
        
        while self.running:
            try:
                async with RedditScraper() as scraper:
                    posts = await scraper.scrape_all(limit_per_sub=self.reddit_limit // 8)
                    
                    for post in posts:
                        self.producer.publish_post(post.to_dict(), 'reddit')
                    
                    self.stats['reddit_scrapes'] += 1
                    self.stats['reddit_posts'] += len(posts)
                    self.stats['last_reddit'] = datetime.now().isoformat()
                    
                    print(f"📤 Reddit: Published {len(posts)} posts (total: {self.stats['reddit_posts']})")
            
            except Exception as e:
                print(f"❌ Reddit scrape error: {e}")
            
            await asyncio.sleep(self.reddit_interval)
    
    async def scrape_hn_loop(self):
        """Continuously scrape HackerNews."""
        print(f"🟠 HN scraper started (every {self.hn_interval}s)")
        
        while self.running:
            try:
                async with HackerNewsScraper() as scraper:
                    stories = await scraper.scrape_all(limit_per_type=self.hn_limit // 5)
                    
                    for story in stories:
                        self.producer.publish_post(story.to_dict(), 'hackernews')
                    
                    self.stats['hn_scrapes'] += 1
                    self.stats['hn_posts'] += len(stories)
                    self.stats['last_hn'] = datetime.now().isoformat()
                    
                    print(f"📤 HN: Published {len(stories)} stories (total: {self.stats['hn_posts']})")
            
            except Exception as e:
                print(f"❌ HN scrape error: {e}")
            
            await asyncio.sleep(self.hn_interval)
    
    async def trending_loop(self):
        """Publish trending snapshots every hour."""
        print("📈 Trending publisher started (every hour)")
        
        while self.running:
            await asyncio.sleep(3600)  # 1 hour
            
            try:
                self.producer.publish_trending({
                    'timestamp': datetime.now().isoformat(),
                    'stats': self.stats.copy()
                })
                print("📈 Published trending snapshot")
            except Exception as e:
                print(f"❌ Trending publish error: {e}")
    
    async def stats_loop(self):
        """Print stats every minute."""
        while self.running:
            await asyncio.sleep(60)
            
            uptime = (datetime.now() - datetime.fromisoformat(self.stats['started_at'])).seconds // 60
            print(f"""
📊 Stats (uptime: {uptime}m)
   Reddit: {self.stats['reddit_posts']} posts ({self.stats['reddit_scrapes']} scrapes)
   HN: {self.stats['hn_posts']} posts ({self.stats['hn_scrapes']} scrapes)
""")
    
    async def run(self):
        """Run all scrapers concurrently."""
        print("""
╔═══════════════════════════════════════════════════════════════╗
║                STREAMING SCRAPER STARTED                       ║
╚═══════════════════════════════════════════════════════════════╝
        """)
        
        tasks = [
            asyncio.create_task(self.scrape_reddit_loop()),
            asyncio.create_task(self.scrape_hn_loop()),
            asyncio.create_task(self.trending_loop()),
            asyncio.create_task(self.stats_loop())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            print("⏹️ Tasks cancelled")
    
    def stop(self):
        """Stop all scrapers."""
        self.running = False
        self.producer.close()
        print("⏹️ Scraper stopped")


async def main():
    """Run the streaming scraper."""
    scraper = StreamingScraper(
        reddit_interval=int(os.getenv("REDDIT_INTERVAL", 300)),
        hn_interval=int(os.getenv("HN_INTERVAL", 180)),
        reddit_limit=int(os.getenv("REDDIT_LIMIT", 50)),
        hn_limit=int(os.getenv("HN_LIMIT", 50))
    )
    
    def signal_handler(sig, frame):
        print("\n⏹️ Shutting down...")
        scraper.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
