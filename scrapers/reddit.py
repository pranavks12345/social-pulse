"""
Reddit Scraper
==============
Async scraper for Reddit posts and comments.
Collects from multiple subreddits, extracts metadata, handles rate limiting.
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
import hashlib


@dataclass
class RedditPost:
    id: str
    subreddit: str
    title: str
    body: str
    author: str
    score: int
    upvote_ratio: float
    num_comments: int
    created_utc: datetime
    url: str
    is_self: bool
    scraped_at: datetime
    
    def to_dict(self):
        d = asdict(self)
        d['created_utc'] = self.created_utc.isoformat()
        d['scraped_at'] = self.scraped_at.isoformat()
        return d


class RateLimiter:
    def __init__(self, requests_per_minute: int = 30):
        self.rpm = requests_per_minute
        self.tokens = requests_per_minute
        self.last_update = time.time()
        self.lock = asyncio.Lock()
    
    async def acquire(self):
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.rpm, self.tokens + elapsed * (self.rpm / 60))
            self.last_update = now
            
            if self.tokens < 1:
                wait_time = (1 - self.tokens) * (60 / self.rpm)
                await asyncio.sleep(wait_time)
                self.tokens = 1
            
            self.tokens -= 1


class RedditScraper:
    BASE_URL = "https://www.reddit.com"
    
    # Subreddits to track for trends
    SUBREDDITS = [
        # Tech
        "technology", "programming", "machinelearning", "artificial",
        "datascience", "Python", "javascript", "webdev",
        # News/Discussion
        "news", "worldnews", "politics", "economics",
        # Finance
        "wallstreetbets", "stocks", "cryptocurrency", "bitcoin",
        # Culture
        "movies", "gaming", "music", "television",
        # General
        "AskReddit", "todayilearned", "science", "space"
    ]
    
    def __init__(self):
        self.limiter = RateLimiter(30)
        self.session: Optional[aiohttp.ClientSession] = None
        self.seen_ids = set()
    
    async def __aenter__(self):
        headers = {
            "User-Agent": "SocialPulse/1.0 (Research Project)"
        }
        self.session = aiohttp.ClientSession(headers=headers)
        return self
    
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
    
    async def scrape_subreddit(self, subreddit: str, limit: int = 100, sort: str = "hot") -> List[RedditPost]:
        """Scrape posts from a subreddit."""
        posts = []
        after = None
        
        while len(posts) < limit:
            await self.limiter.acquire()
            
            url = f"{self.BASE_URL}/r/{subreddit}/{sort}.json"
            params = {"limit": min(100, limit - len(posts)), "raw_json": 1}
            if after:
                params["after"] = after
            
            try:
                async with self.session.get(url, params=params, timeout=10) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(60)
                        continue
                    if resp.status != 200:
                        break
                    
                    data = await resp.json()
            except Exception as e:
                print(f"  Error scraping r/{subreddit}: {e}")
                break
            
            children = data.get("data", {}).get("children", [])
            if not children:
                break
            
            for child in children:
                post_data = child.get("data", {})
                post_id = post_data.get("id")
                
                if post_id in self.seen_ids:
                    continue
                self.seen_ids.add(post_id)
                
                post = RedditPost(
                    id=post_id,
                    subreddit=subreddit,
                    title=post_data.get("title", ""),
                    body=post_data.get("selftext", ""),
                    author=post_data.get("author", "[deleted]"),
                    score=post_data.get("score", 0),
                    upvote_ratio=post_data.get("upvote_ratio", 0),
                    num_comments=post_data.get("num_comments", 0),
                    created_utc=datetime.fromtimestamp(post_data.get("created_utc", 0)),
                    url=post_data.get("url", ""),
                    is_self=post_data.get("is_self", False),
                    scraped_at=datetime.now()
                )
                posts.append(post)
            
            after = data.get("data", {}).get("after")
            if not after:
                break
        
        return posts
    
    async def scrape_all(self, limit_per_sub: int = 50) -> List[RedditPost]:
        """Scrape all tracked subreddits."""
        all_posts = []
        
        for subreddit in self.SUBREDDITS:
            print(f"  Scraping r/{subreddit}...")
            posts = await self.scrape_subreddit(subreddit, limit=limit_per_sub)
            all_posts.extend(posts)
            print(f"    Got {len(posts)} posts")
        
        return all_posts
    
    async def scrape_trending(self) -> List[RedditPost]:
        """Scrape trending/rising posts across subreddits."""
        all_posts = []
        
        for subreddit in self.SUBREDDITS[:10]:  # Top 10 for trending
            await self.limiter.acquire()
            
            for sort in ["rising", "hot"]:
                posts = await self.scrape_subreddit(subreddit, limit=25, sort=sort)
                all_posts.extend(posts)
        
        return all_posts


async def main():
    """Test scraper."""
    print("Testing Reddit Scraper...")
    
    async with RedditScraper() as scraper:
        posts = await scraper.scrape_all(limit_per_sub=10)
        
        print(f"\nTotal posts: {len(posts)}")
        print("\nSample posts:")
        for post in posts[:5]:
            print(f"  [{post.subreddit}] {post.title[:60]}... (score: {post.score})")
    
    return posts


if __name__ == "__main__":
    asyncio.run(main())
