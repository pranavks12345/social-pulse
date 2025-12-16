-- Social Pulse Database Initialization
-- Creates tables and indexes for the data pipeline

-- Posts table
CREATE TABLE IF NOT EXISTS posts (
    id SERIAL PRIMARY KEY,
    external_id VARCHAR(100) UNIQUE NOT NULL,
    source VARCHAR(50) NOT NULL,
    
    title TEXT,
    body TEXT,
    url TEXT,
    author VARCHAR(200),
    
    score INTEGER DEFAULT 0,
    num_comments INTEGER DEFAULT 0,
    upvote_ratio FLOAT,
    
    subreddit VARCHAR(100),
    story_type VARCHAR(50),
    
    created_at TIMESTAMP,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    sentiment_score FLOAT,
    sentiment_label VARCHAR(20),
    topics JSONB,
    keywords JSONB,
    entities JSONB,
    viral_score FLOAT,
    engagement_prediction VARCHAR(20)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_posts_source ON posts(source);
CREATE INDEX IF NOT EXISTS idx_posts_subreddit ON posts(subreddit);
CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_at);
CREATE INDEX IF NOT EXISTS idx_posts_scraped ON posts(scraped_at);
CREATE INDEX IF NOT EXISTS idx_posts_sentiment ON posts(sentiment_label);
CREATE INDEX IF NOT EXISTS idx_posts_viral ON posts(viral_score);
CREATE INDEX IF NOT EXISTS idx_posts_source_created ON posts(source, created_at);

-- Trend snapshots table
CREATE TABLE IF NOT EXISTS trend_snapshots (
    id SERIAL PRIMARY KEY,
    snapshot_time TIMESTAMP NOT NULL,
    source VARCHAR(50),
    topic VARCHAR(100),
    post_count INTEGER,
    avg_score FLOAT,
    avg_sentiment FLOAT,
    avg_viral_score FLOAT,
    top_keywords JSONB
);

CREATE INDEX IF NOT EXISTS idx_snapshots_time ON trend_snapshots(snapshot_time);
CREATE INDEX IF NOT EXISTS idx_snapshots_topic ON trend_snapshots(topic);

-- Top entities table
CREATE TABLE IF NOT EXISTS top_entities (
    id SERIAL PRIMARY KEY,
    snapshot_time TIMESTAMP NOT NULL,
    entity_text VARCHAR(200),
    entity_type VARCHAR(50),
    mention_count INTEGER,
    avg_sentiment FLOAT,
    sources JSONB
);

CREATE INDEX IF NOT EXISTS idx_entities_time ON top_entities(snapshot_time);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pulse;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO pulse;
