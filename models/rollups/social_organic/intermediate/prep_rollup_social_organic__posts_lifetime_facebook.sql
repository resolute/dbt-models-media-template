{{ config(enabled= (var('facebook_organic_ids'))|length > 0 is true) }}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_facebook_organic__post') }}

),
  
general_definitions AS (
    
    SELECT
    
        *,
        'Facebook Organic' AS data_source,
        'Facebook' AS channel_source_name,
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
        post_id AS post_id,
        created_time AS post_publish_date,
        type AS post_type,
        permalink AS post_permalink_url,
        post_image_url AS post_thumbnail_url,
        message AS post_message,
        attachments_unshimmed_url AS post_destination_link_url,
        post_video_length AS post_video_length,
        
        post_impressions_unique AS impressions_unique,
        post_impressions AS impressions,
        (total_comments + shares + post_clicks + post_reactions_anger_total + post_reactions_haha_total + post_reactions_like_total + post_reactions_love_total + post_reactions_sorry_total + post_reactions_wow_total) AS engagements,
        total_comments AS comments,
        shares AS shares,
        (post_reactions_anger_total + post_reactions_haha_total + post_reactions_like_total + post_reactions_love_total + post_reactions_sorry_total + post_reactions_wow_total) AS likes,
        post_clicks_by_link_clicks AS link_clicks,
        post_video_views AS video_views,
        (post_video_complete_views_organic + post_video_complete_views_organic) AS video_completions,
        post_video_view_time AS video_view_time
        
     FROM general_definitions

),

calculate_post_age AS (

    SELECT
        
        *,
        DATE_DIFF(date, post_publish_date, DAY) AS post_age_in_days
    
    FROM rename_columns_and_set_defaults

),

final AS (

    SELECT *
    
    FROM calculate_post_age

)
      
SELECT * FROM final
