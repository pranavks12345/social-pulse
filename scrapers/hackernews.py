"""
HackerNews Scraper
==================
Async scraper using HackerNews official API.
Collects top, new, best, and ask stories.
"""

import asyncio
import aiohttp
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict


@dataclass
class HNStory:
    id: int
    title: str
    url: str
    text: str
    author: str
    score: int
    num_comments: int
    created_utc: datetime
    story_type: str  # top, new, best, ask, show
    scraped_at: datetime
    
    def to_dict(self):
        d = asdict(self)
        d['created_utc'] = self.created_utc.isoformat()
        d['scraped_at'] = self.scraped_at.isoformat()
        return d


class HackerNewsScraper:
    BASE_URL = "https://hacker-news.firebaseio.com/v0"
    
    ENDPOINTS = {
        "top": "topstories",
        "new": "newstories",
        "best": "beststories",
        "ask": "askstories",
        "show": "showstories"
    }
    
    def __init__(self, max_concurrent: int = 20):
        self.max_concurrent = max_concurrent
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
    
    async def get_story_ids(self, story_type: str = "top", limit: int = 100) -> List[int]:
        """Get story IDs for a given type."""
        endpoint = self.ENDPOINTS.get(story_type, "topstories")
        url = f"{self.BASE_URL}/{endpoint}.json"
        
        try:
            async with self.session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    return []
                ids = await resp.json()
                return ids[:limit]
        except Exception as e:
            print(f"  Error getting {story_type} IDs: {e}")
            return []
    
    async def get_story(self, story_id: int, story_type: str = "top") -> Optional[HNStory]:
        """Fetch a single story by ID."""
        async with self.semaphore:
            url = f"{self.BASE_URL}/item/{story_id}.json"
            
            try:
                async with self.session.get(url, timeout=10) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    
                    if not data or data.get("type") != "story":
                        return None
                    
                    return HNStory(
                        id=data.get("id", 0),
                        title=data.get("title", ""),
                        url=data.get("url", ""),
                        text=data.get("text", "") or "",
                        author=data.get("by", "[unknown]"),
                        score=data.get("score", 0),
                        num_comments=data.get("descendants", 0) or 0,
                        created_utc=datetime.fromtimestamp(data.get("time", 0)),
                        story_type=story_type,
                        scraped_at=datetime.now()
                    )
            except Exception as e:
                return None
    
    async def scrape_stories(self, story_type: str = "top", limit: int = 100) -> List[HNStory]:
        """Scrape stories of a given type."""
        ids = await self.get_story_ids(story_type, limit)
        
        tasks = [self.get_story(sid, story_type) for sid in ids]
        results = await asyncio.gather(*tasks)
        
        return [s for s in results if s is not None]
    
    async def scrape_all(self, limit_per_type: int = 50) -> List[HNStory]:
        """Scrape all story types."""
        all_stories = []
        
        for story_type in self.ENDPOINTS.keys():
            print(f"  Scraping HN {story_type}...")
            stories = await self.scrape_stories(story_type, limit_per_type)
            all_stories.extend(stories)
            print(f"    Got {len(stories)} stories")
        
        # Deduplicate by ID
        seen = set()
        unique = []
        for story in all_stories:
            if story.id not in seen:
                seen.add(story.id)
                unique.append(story)
        
        return unique


async def main():
    """Test scraper."""
    print("Testing HackerNews Scraper...")
    
    async with HackerNewsScraper() as scraper:
        stories = await scraper.scrape_all(limit_per_type=20)
        
        print(f"\nTotal stories: {len(stories)}")
        print("\nTop stories by score:")
        sorted_stories = sorted(stories, key=lambda x: x.score, reverse=True)
        for story in sorted_stories[:5]:
            print(f"  [{story.score}] {story.title[:60]}...")
    
    return stories


if __name__ == "__main__":
    asyncio.run(main())
