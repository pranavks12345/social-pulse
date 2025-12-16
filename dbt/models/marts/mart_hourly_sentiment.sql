-- Mart: Hourly sentiment tracking
-- Track sentiment changes hour by hour

with posts as (
    select * from {{ ref('int_post_metrics') }}
),

hourly_sentiment as (
    select
        created_date,
        created_hour,
        source,
        
        count(*) as post_count,
        
        avg(sentiment_score) as avg_sentiment,
        min(sentiment_score) as min_sentiment,
        max(sentiment_score) as max_sentiment,
        
        -- Sentiment distribution
        sum(case when sentiment_category = 'very_positive' then 1 else 0 end) as very_positive,
        sum(case when sentiment_category = 'positive' then 1 else 0 end) as positive,
        sum(case when sentiment_category = 'neutral' then 1 else 0 end) as neutral,
        sum(case when sentiment_category = 'negative' then 1 else 0 end) as negative,
        sum(case when sentiment_category = 'very_negative' then 1 else 0 end) as very_negative,
        
        -- Avg metrics for context
        avg(score) as avg_score,
        avg(viral_score) as avg_viral

    from posts
    group by created_date, created_hour, source
)

select
    *,
    datetime(created_date || ' ' || created_hour || ':00:00') as hour_timestamp,
    (very_positive + positive - negative - very_negative) * 1.0 / nullif(post_count, 0) as sentiment_index
from hourly_sentiment
order by created_date desc, created_hour desc, source
