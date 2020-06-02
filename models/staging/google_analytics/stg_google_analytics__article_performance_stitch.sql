WITH

source_data AS (
  
    SELECT * FROM {{ source('google_analytics_stitch_yale_news', 'ga_article_performance_stitch') }}

),

recast_rename AS (
  
    SELECT

        profile_id AS account_id,
        CONCAT('Yale News (UA-991898-10) -> ', '1 - Yale News') AS account_name,
        DATE(start_date) AS date,
        ga_dimension3 AS website_article_id,
        ga_pagepath AS website_page_path,
        ga_channelgrouping AS website_channel_group,
        TRIM(SPLIT(ga_sourcemedium, '/')[OFFSET(0)]) AS utm_source,
        TRIM(SPLIT(ga_sourcemedium, '/')[OFFSET(1)]) AS utm_medium,
        ga_campaign AS utm_campaign,
        ga_adcontent AS utm_content,
        ga_keyword AS utm_term,
      
        CAST(ga_metric1 AS INT64) AS website_article_views,
        CAST(ga_metric2 AS INT64) AS website_article_read,
        CAST(ga_metric3 AS INT64) AS website_article_shares,
        CAST(CAST(ga_metric4 AS FLOAT64) AS INT64) AS website_article_engaged_time_total
    
    FROM source_data

),

final AS (

    SELECT

        {{ dbt_utils.surrogate_key(['date', 'account_id', 'website_article_id', 'website_page_path', 'website_channel_group', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term']) }} AS id,
        *
    
    FROM recast_rename

)

SELECT * FROM final