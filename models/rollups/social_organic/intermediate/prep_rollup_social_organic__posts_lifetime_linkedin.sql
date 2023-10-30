{{ config(enabled= get_account_ids('linkedin organic')|length > 0 is true) }}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_linkedin_organic__share_lifetime') }}

),
  
general_definitions AS (
    
    SELECT
    
        *,
        'LinkedIn Organic' AS data_source,
        'LinkedIn' AS channel_source_name,
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
        share AS post_id,
        DATE(CAST(created_date AS DATETIME)) AS post_publish_date,
        distribution_feed_distribution AS post_type,
        post_in_the_feed_url AS post_permalink_url,
        '(not set)' AS post_thumbnail_url,
        commentary AS post_message,
        '(not set)' AS post_destination_link_url,
        0 AS post_video_length,
        
        0 AS impressions_unique,
        impressions AS impressions,
        (clicks + likes + shares + comments) AS engagements,
        comments,
        shares,
        likes,
        clicks AS link_clicks,
        0 AS video_views,
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
