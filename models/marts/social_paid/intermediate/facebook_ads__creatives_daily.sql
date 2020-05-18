WITH

data AS (
  
    SELECT * FROM {{ ref('stg_facebook_ads__creative') }}

),
  
extract_utm_params AS (

    SELECT
    
        *,
        REGEXP_EXTRACT(website_destination_url,r'[?&]utm_source=([^&]+)') as utm_source,
        REGEXP_EXTRACT(website_destination_url,r'[?&]utm_medium=([^&]+)') as utm_medium,
        REGEXP_EXTRACT(website_destination_url,r'[?&]utm_campaign=([^&]+)') as utm_campaign,
        REGEXP_EXTRACT(website_destination_url,r'[?&]utm_content=([^&]+)') as utm_content,
        REGEXP_EXTRACT(website_destination_url,r'[?&]utm_term=([^&]+)') as utm_term
    
    FROM data
    
),
  
general_definitions AS (

    SELECT
    
        *,
        'Facebook Paid' AS data_source,
        'Facebook' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Social' AS channel_name
  
    FROM extract_utm_params
    
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
        campaign_id,
        campaign_name,
        adset_id,
        adset_name,
        ad_id,
        ad_name,
        creative_id,
        creative_name,
        objective AS ad_objective,
        DATE(CAST(publication_date AS DATETIME)) AS ad_publish_date,
        object_type AS ad_type,
        creative_link AS ad_permalink_url,
        image_url AS ad_thumbnail_url,
        body AS ad_message,
        website_destination_url AS ad_destination_link_url,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        
        reach AS impressions_unique,
        impressions AS impressions,
        (post_reactions + clicks + comments + shares) AS engagements,
        outbound_clicks AS link_clicks,
        spend AS cost,
        video_view_3s AS video_views,
        video_p95_watched_actions AS video_completions
        
     FROM general_definitions

),

calculate_ad_age AS (

    SELECT
        
        *,
        DATE_DIFF(date, ad_publish_date, DAY) AS ad_age_in_days
    
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
        date,
        campaign_id,
        campaign_name,
        adset_id,
        adset_name,
        ad_id,
        ad_name,
        creative_id,
        creative_name,
        ad_objective,
        ad_publish_date,
        ad_type,
        ad_permalink_url,
        ad_thumbnail_url,
        ad_message,
        ad_destination_link_url,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        ad_age_in_days,
        
        SUM(impressions_unique) AS impressions_unique,
        SUM(impressions) AS impressions,
        SUM(engagements) AS engagements,
        SUM(link_clicks) AS link_clicks,
        SUM(cost) AS cost,
        SUM(video_views) AS video_views,
        SUM(video_completions) AS video_completions
        
     FROM calculate_ad_age
     
     GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28

)
  
SELECT * FROM final