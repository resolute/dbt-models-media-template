WITH

source_data as (

    SELECT * FROM {{ source('google_analytics', 'ga_alumni_events_page_performance') }}

),

final AS (
  
    SELECT
    
        {{ dbt_utils.surrogate_key(['Date', 'View_ID', 'Page_path', 'Channel_grouping', 'Source', 'Medium', 'Campaign', 'Ad_content', 'Keyword']) }} AS id,
        CAST(View_ID AS STRING) AS account_id,
        CONCAT('alumni.yale.edu (UA-58620247-1) -> ', LTRIM(View, 'alumni.yale.edu: ')) AS account_name,
        CAST(Date AS DATE) AS date,
        Page_path AS website_page_path,
        Channel_grouping AS website_channel_group,
        Source AS utm_source,
        Medium AS utm_medium,
        Campaign AS utm_campaign,
        Ad_content AS utm_content,
        Keyword AS utm_term,
      
        Pageviews AS website_pageviews,
        Unique_pageviews AS website_pageviews_unique
    
    FROM source_data

)

SELECT * FROM final