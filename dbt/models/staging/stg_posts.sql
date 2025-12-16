-- Staging model: Clean and standardize raw posts
-- This model reads from the posts table and does initial cleanup

with source as (
    select * from {{ source('social_pulse', 'posts') }}
),

cleaned as (
    select
        id,
        external_id,
        source,
        
        -- Clean title and body
        trim(title) as title,
        trim(coalesce(body, '')) as body,
        length(trim(title)) as title_length,
        length(trim(coalesce(body, ''))) as body_length,
        
        url,
        author,
        
        -- Metrics
        coalesce(score, 0) as score,
        coalesce(num_comments, 0) as num_comments,
        upvote_ratio,
        
        -- Source-specific
        subreddit,
        story_type,
        
        -- Timestamps
        created_at,
        scraped_at,
        
        -- Calculate age in hours
        (julianday(scraped_at) - julianday(created_at)) * 24 as age_hours,
        
        -- NLP fields
        sentiment_score,
        sentiment_label,
        topics,
        keywords,
        entities,
        viral_score,
        engagement_prediction,
        
        -- Date parts for aggregation
        date(created_at) as created_date,
        strftime('%H', created_at) as created_hour,
        strftime('%w', created_at) as day_of_week

    from source
    where title is not null and length(trim(title)) > 0
)

select * from cleaned
