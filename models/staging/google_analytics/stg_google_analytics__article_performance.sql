WITH

source_data AS (
  
    SELECT * FROM {{ source('google_analytics', 'ga_article_performance_combined') }}

),

final AS (
  
    SELECT
    
        CAST(View_ID AS STRING) AS account_id,
        CONCAT('Yale News (UA-991898-10) -> ', View) AS account_name,
        CAST(Date AS DATE) AS date,
        CAST(Custom_dimension_3 AS STRING) AS website_article_id,
        Page_path AS website_page_path,
        Channel_grouping AS website_channel_group,
        Source AS utm_source,
        Medium AS utm_medium,
        Campaign AS utm_campaign,
        Ad_content AS utm_content,
        Keyword AS utm_term,
      
        Custom_metric_1 AS website_article_views,
        Custom_metric_2 AS website_article_read,
        Custom_metric_3 AS website_article_shares,
        Custom_metric_4 AS website_article_engaged_time_total
    
    FROM source_data

)

SELECT * FROM final