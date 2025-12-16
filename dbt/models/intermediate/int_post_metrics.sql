-- Intermediate model: Post metrics and calculations

with posts as (
    select * from {{ ref('stg_posts') }}
),

with_metrics as (
    select
        *,
        
        -- Engagement rate (comments per score)
        case 
            when score > 0 then num_comments * 1.0 / score 
            else 0 
        end as engagement_rate,
        
        -- Velocity (score per hour)
        case 
            when age_hours > 0 then score * 1.0 / age_hours 
            else score 
        end as velocity,
        
        -- Sentiment category
        case
            when sentiment_score >= 0.5 then 'very_positive'
            when sentiment_score >= 0.1 then 'positive'
            when sentiment_score <= -0.5 then 'very_negative'
            when sentiment_score <= -0.1 then 'negative'
            else 'neutral'
        end as sentiment_category,
        
        -- Viral category
        case
            when viral_score >= 0.8 then 'viral'
            when viral_score >= 0.6 then 'trending'
            when viral_score >= 0.4 then 'engaging'
            else 'standard'
        end as viral_category,
        
        -- Content type
        case
            when body_length > 500 then 'long_form'
            when body_length > 100 then 'medium'
            when body_length > 0 then 'short'
            else 'link_only'
        end as content_type,
        
        -- Is high performer
        case when score > 100 or viral_score > 0.6 then 1 else 0 end as is_high_performer

    from posts
)

select * from with_metrics
