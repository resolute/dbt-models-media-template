WITH

data AS (
  
    SELECT * FROM {{ ref('stg_google_ads__search_query_extended') }}

),
  
general_definitions AS (

    SELECT
    
        *,
        'Google Ads' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name
  
    FROM data
    
),

rename_columns_and_set_defaults AS (

    SELECT
    
        id,
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        date,
        customer_id,
        account_descriptive_name AS account_desc_name,
        campaign_id,
        campaign_name,
        campaign_type,
        adset_id,
        adset_name,
        keyword_id,
        keyword_name,
        ad_network_type_1,
        search_term,
        query_match_type,
        avg_pos,
        
        imps AS impressions,
        clicks AS link_clicks,
        spend AS cost,
        engagements,
        interactions,
        views,
        video_quartile_25,
        video_quartile_50,
        video_quartile_75,
        video_quartile_100
        
     FROM general_definitions

),

-- Find post age in days
final AS (

    SELECT
        
        *
    
    FROM rename_columns_and_set_defaults

)
  
SELECT * FROM final