WITH

source_data as (

    SELECT * FROM {{ source('google_analytics_stitch_yale_alumni', 'ga_alumni_events_page_performance_stitch') }}

),

filter AS (

    SELECT *
    
    FROM source_data

    WHERE ga_pagetitle = 'Events Calendar | Yale Alumni Association'

),

recast_rename AS (
  
    SELECT
    
        profile_id AS account_id,
        CONCAT('alumni.yale.edu (UA-58620247-1) -> ', 'Exclude Yale University Traffic') AS account_name,
        DATE(start_date) AS date,
        ga_pagepath AS website_page_path,
        ga_channelgrouping AS website_channel_group,
        TRIM(SPLIT(ga_sourcemedium, '/')[OFFSET(0)]) AS utm_source,
        TRIM(SPLIT(ga_sourcemedium, '/')[OFFSET(1)]) AS utm_medium,
        ga_campaign AS utm_campaign,
        ga_adcontent AS utm_content,
        ga_keyword AS utm_term,
      
        ga_pageviews AS website_pageviews,
        ga_uniquepageviews AS website_pageviews_unique
    
    FROM filter

),

final AS (

    SELECT

        {{ dbt_utils.surrogate_key(['date', 'account_id', 'website_page_path', 'website_channel_group', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term']) }} AS id,
        *

    FROM recast_rename

)

SELECT * FROM final
