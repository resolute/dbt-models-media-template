{{ config(enabled= (var('instagram_organic_ids'))|length > 0 is true) }}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_instagram_organic__post') }}

),
  
general_definitions AS (
    
    SELECT
    
        *,
        'Instagram Organic' AS data_source,
        'Instagram' AS channel_source_name,
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
        media_id AS post_id,
        CAST(publication_date AS DATE) AS post_publish_date,
        media_type AS post_type,
        media_post_link_url AS post_permalink_url,
        media_url AS post_thumbnail_url,
        caption AS post_message,
        '(not set)' AS post_destination_link_url,
        0 AS post_video_length,
        
        reach AS impressions_unique,
        impressions AS impressions,
        engagement AS engagements,
        comments_count AS comments,
        0 shares,
        like_count AS likes,
        0 AS link_clicks,
        video_views AS video_views,
        0 AS video_completions,
        0 AS video_view_time
        
     FROM general_definitions

),

calculate_post_age_and_most_recent_data_rank AS (

    SELECT
        
        *,
        DATE_DIFF(date, post_publish_date, DAY) AS post_age_in_days,
        ROW_NUMBER() OVER (PARTITION BY account_id, post_id ORDER BY date DESC, post_permalink_url DESC) AS post_most_recent_data_rank
    
    FROM rename_columns_and_set_defaults

),

final AS (

    SELECT * EXCEPT(post_most_recent_data_rank)
    
    FROM calculate_post_age_and_most_recent_data_rank
    
    WHERE post_most_recent_data_rank = 1

)
      
SELECT * FROM final
