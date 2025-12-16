-- Mart: Top posts by various metrics
-- Identify best performing content

with posts as (
    select * from {{ ref('int_post_metrics') }}
),

ranked_posts as (
    select
        *,
        row_number() over (partition by source order by score desc) as rank_by_score,
        row_number() over (partition by source order by viral_score desc) as rank_by_viral,
        row_number() over (partition by source order by velocity desc) as rank_by_velocity,
        row_number() over (partition by source order by engagement_rate desc) as rank_by_engagement
    from posts
    where age_hours <= 48  -- Last 48 hours only
)

select
    id,
    external_id,
    source,
    subreddit,
    title,
    url,
    author,
    score,
    num_comments,
    sentiment_score,
    sentiment_label,
    viral_score,
    viral_category,
    velocity,
    engagement_rate,
    age_hours,
    created_at,
    topics,
    keywords,
    
    -- Rankings
    rank_by_score,
    rank_by_viral,
    rank_by_velocity,
    rank_by_engagement,
    
    -- Is top in any category
    case when rank_by_score <= 10 
         or rank_by_viral <= 10 
         or rank_by_velocity <= 10 
    then 1 else 0 end as is_top_post

from ranked_posts
where rank_by_score <= 100  -- Keep top 100 per source
order by score desc
