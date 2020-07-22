{%- set source_account_ids = var('google_ads_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_adwords_search_query_extended') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        account_id,
        account_name,
        date,
        customer_id,
        account_descriptive_name AS account_desc_name,
        campaign_id,
        campaign_name,
        campaign_type,
        adset_id AS ad_group_id,
        adset_name AS ad_group_name,
        keyword_id,
        keyword_name,
        ad_network_type_1,
        search_term,
        query_match_type,
        avg_pos,
        
        {#- General metrics -#}
        imps AS impressions,
        clicks AS link_clicks,
        spend AS cost,
        engagements,
        interactions,

        {#- Video metrics -#}
        views AS video_views,
        video_quartile_25 AS video_p25_watched,
        video_quartile_50 AS video_p50_watched,
        video_quartile_75 AS video_p75_watched,
        video_quartile_100 AS video_completions,

        -- Excluded fields --
        /*
        conv,
        revenue,
        conversions,
        conversion_value,
        view_through_conv,
        conversion_rate,
        */

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_id', 'ad_group_id', 'keyword_id', 'search_term', 'query_match_type']) }} AS id,
        'Google Ads' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final