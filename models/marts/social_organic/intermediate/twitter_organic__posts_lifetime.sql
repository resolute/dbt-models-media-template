{{-
    config(
        enabled = var('twitter_organic_ids') != None
    )
-}}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_twitter_organic__tweets') }}

),
  
general_definitions AS (
    
    SELECT
    
        *,
        'Twitter Organic' AS data_source,
        'Twitter' AS channel_source_name,
        'Organic' AS channel_source_type,
        'Organic Social' AS channel_name
        
    FROM data

),
  
rename_columns_and_set_defaults AS (

    SELECT
    
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        date,
        organic_tweet_id AS post_id,
        CAST(created_at AS DATE) AS post_publish_date,
        '(not set)' AS post_type,
        url AS post_permalink_url,
        '(not set)' AS post_thumbnail_url,
        text AS post_message,
        '(not set)' AS post_destination_link_url,
        0 AS post_video_length,
        
        0 AS impressions_unique,
        impressions AS impressions,
        engagements AS engagements,
        app_clicks AS link_clicks,
        video_mrc_views AS video_views,
        video_3s100pct_views AS video_completions,
        0 AS video_view_time
        
     FROM general_definitions

),

calculate_post_age AS (

    SELECT
        
        *,
        DATE_DIFF(date, post_publish_date, DAY) AS post_age_in_days
    
    FROM rename_columns_and_set_defaults

),

final AS (

    SELECT 
    
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        MAX(date) AS date,
        post_id,
        post_publish_date,
        post_type,
        post_permalink_url,
        post_thumbnail_url,
        post_message,
        post_destination_link_url,
        post_video_length,
        
        SUM(impressions_unique) AS impressions_unique,
        SUM(impressions) AS impressions,
        SUM(engagements) AS engagements,
        SUM(link_clicks) AS link_clicks,
        SUM(video_views) AS video_views,
        SUM(video_completions) AS video_completions,
        SUM(video_view_time) AS video_view_time,
        
        MAX(post_age_in_days) AS post_age_in_days
    
    FROM calculate_post_age
    
    GROUP BY 1,2,3,4,5,6,8,9,10,11,12,13,14,15

)
      
SELECT * FROM final