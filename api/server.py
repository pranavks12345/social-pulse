"""
Real-time API
=============
FastAPI server with real-time WebSocket streaming for live updates.
"""

import os
import sys
import json
import asyncio
from datetime import datetime, timedelta
from typing import List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import redis.asyncio as redis

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import db, Post


# Redis connection
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
redis_client = None


class ConnectionManager:
    """WebSocket connection manager."""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    
    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass


manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = await redis.from_url(REDIS_URL)
    db.create_tables()
    print("🚀 API started")
    yield
    if redis_client:
        await redis_client.close()
    print("👋 API stopped")


app = FastAPI(
    title="Social Pulse API",
    description="Real-time social media analytics API",
    version="2.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class PostResponse(BaseModel):
    id: int
    source: str
    title: str
    score: int
    sentiment_score: Optional[float]
    sentiment_label: Optional[str]
    viral_score: Optional[float]
    topics: Optional[List[str]]
    created_at: Optional[datetime]


class TrendingResponse(BaseModel):
    topic: str
    post_count: int
    avg_score: float
    avg_sentiment: float


class StatsResponse(BaseModel):
    total_posts: int
    posts_24h: int
    avg_sentiment: float
    positive_pct: float
    negative_pct: float
    top_viral_score: float


@app.get("/")
async def root():
    return {
        "name": "Social Pulse API",
        "version": "2.0",
        "endpoints": ["/posts", "/trending", "/stats", "/search", "/ws"]
    }


@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.get("/posts", response_model=List[PostResponse])
async def get_posts(
    source: Optional[str] = None,
    sentiment: Optional[str] = None,
    min_score: int = 0,
    hours: int = 24,
    limit: int = 100
):
    """Get recent posts with filters."""
    with db.session() as session:
        posts = db.get_recent_posts(session, source=source, hours=hours, limit=limit)
        
        # Apply filters
        result = []
        for p in posts:
            if p.score < min_score:
                continue
            if sentiment and p.sentiment_label != sentiment:
                continue
            
            result.append(PostResponse(
                id=p.id,
                source=p.source,
                title=p.title,
                score=p.score or 0,
                sentiment_score=p.sentiment_score,
                sentiment_label=p.sentiment_label,
                viral_score=p.viral_score,
                topics=p.topics,
                created_at=p.created_at
            ))
        
        return result


@app.get("/trending", response_model=List[TrendingResponse])
async def get_trending(hours: int = 24, limit: int = 10):
    """Get trending topics."""
    with db.session() as session:
        topics = db.get_trending_topics(session, hours=hours)
        
        return [
            TrendingResponse(
                topic=t["topic"],
                post_count=t["post_count"],
                avg_score=t["avg_score"],
                avg_sentiment=t["avg_sentiment"]
            )
            for t in topics[:limit]
        ]


@app.get("/stats", response_model=StatsResponse)
async def get_stats(hours: int = 24):
    """Get overall statistics."""
    with db.session() as session:
        posts = db.get_recent_posts(session, hours=hours, limit=10000)
        
        if not posts:
            return StatsResponse(
                total_posts=0,
                posts_24h=0,
                avg_sentiment=0,
                positive_pct=0,
                negative_pct=0,
                top_viral_score=0
            )
        
        sentiments = [p.sentiment_score or 0 for p in posts]
        labels = [p.sentiment_label for p in posts]
        virals = [p.viral_score or 0 for p in posts]
        
        return StatsResponse(
            total_posts=len(posts),
            posts_24h=len(posts),
            avg_sentiment=sum(sentiments) / len(sentiments),
            positive_pct=labels.count("positive") / len(labels) * 100,
            negative_pct=labels.count("negative") / len(labels) * 100,
            top_viral_score=max(virals) if virals else 0
        )


@app.get("/search")
async def search_posts(
    q: str = Query(..., min_length=2),
    hours: int = 24,
    limit: int = 50
):
    """Search posts by keyword."""
    with db.session() as session:
        posts = db.get_recent_posts(session, hours=hours, limit=1000)
        
        q_lower = q.lower()
        results = [
            {
                "id": p.id,
                "source": p.source,
                "title": p.title,
                "score": p.score,
                "sentiment": p.sentiment_label,
                "viral_score": p.viral_score
            }
            for p in posts
            if q_lower in (p.title or "").lower() or q_lower in str(p.keywords or []).lower()
        ][:limit]
        
        return {"query": q, "count": len(results), "results": results}


@app.get("/sentiment/timeline")
async def sentiment_timeline(topic: Optional[str] = None, hours: int = 24):
    """Get sentiment over time."""
    with db.session() as session:
        data = db.get_sentiment_over_time(session, topic=topic, hours=hours)
        return {"topic": topic, "hours": hours, "data": data}


@app.get("/viral")
async def get_viral_posts(min_score: float = 0.6, limit: int = 20):
    """Get viral/trending posts."""
    with db.session() as session:
        posts = db.get_recent_posts(session, hours=48, limit=500)
        
        viral = [
            {
                "id": p.id,
                "source": p.source,
                "title": p.title,
                "score": p.score,
                "viral_score": p.viral_score,
                "sentiment": p.sentiment_label,
                "url": p.url
            }
            for p in posts
            if (p.viral_score or 0) >= min_score
        ]
        
        viral.sort(key=lambda x: x["viral_score"], reverse=True)
        return viral[:limit]


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for real-time updates."""
    await manager.connect(websocket)
    
    try:
        # Send initial data
        with db.session() as session:
            posts = db.get_recent_posts(session, hours=1, limit=10)
            await websocket.send_json({
                "type": "initial",
                "posts": [
                    {"title": p.title, "score": p.score, "sentiment": p.sentiment_label}
                    for p in posts
                ]
            })
        
        # Listen for messages and send updates
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_json(), timeout=30)
                
                if data.get("action") == "subscribe":
                    await websocket.send_json({"type": "subscribed", "topic": data.get("topic")})
                
            except asyncio.TimeoutError:
                # Send heartbeat
                await websocket.send_json({"type": "heartbeat", "ts": datetime.now().isoformat()})
    
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.post("/webhook/new-post")
async def new_post_webhook(post: dict):
    """Webhook endpoint for new post notifications."""
    # Broadcast to all connected clients
    await manager.broadcast({
        "type": "new_post",
        "data": post
    })
    return {"status": "broadcasted"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
