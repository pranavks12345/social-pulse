-- Mart: Daily trend summary
-- Aggregated daily stats by source and topic

with posts as (
    select * from {{ ref('int_post_metrics') }}
),

daily_stats as (
    select
        created_date,
        source,
        
        -- Volume
        count(*) as post_count,
        sum(score) as total_score,
        sum(num_comments) as total_comments,
        
        -- Averages
        avg(score) as avg_score,
        avg(num_comments) as avg_comments,
        avg(sentiment_score) as avg_sentiment,
        avg(viral_score) as avg_viral_score,
        avg(velocity) as avg_velocity,
        
        -- Sentiment breakdown
        sum(case when sentiment_label = 'positive' then 1 else 0 end) as positive_count,
        sum(case when sentiment_label = 'neutral' then 1 else 0 end) as neutral_count,
        sum(case when sentiment_label = 'negative' then 1 else 0 end) as negative_count,
        
        -- High performers
        sum(is_high_performer) as high_performer_count,
        
        -- Viral breakdown
        sum(case when viral_category = 'viral' then 1 else 0 end) as viral_count,
        sum(case when viral_category = 'trending' then 1 else 0 end) as trending_count

    from posts
    group by created_date, source
)

select
    *,
    positive_count * 100.0 / nullif(post_count, 0) as positive_pct,
    negative_count * 100.0 / nullif(post_count, 0) as negative_pct,
    high_performer_count * 100.0 / nullif(post_count, 0) as high_performer_pct
from daily_stats
order by created_date desc, source
