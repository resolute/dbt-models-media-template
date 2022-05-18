{%- set source_account_ids = get_account_ids('google ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_ads_keywords_extended') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        account_id,
        account_name,
        date,
        customer_id,
        campaign_id,
        campaign_name,
        campaign_type,
        campaign_state AS campaign_status,
        adset_id AS ad_group_id,
        adset_name AS ad_group_name,
        adset_state AS ad_group_status,
        keyword_id,
        keyword_name,
        keyword_state AS keyword_status,
        keyword_label_ids,
        keyword_labels,
        match_type,
        ad_network_type,
        
        {#- General metrics -#}
        imps AS impressions,
        search_eligible_impressions,
        spend AS cost,
        clicks AS link_clicks,
        engagements,
        interactions,
        quality_score,

        {#- Video metrics -#}
        views AS video_views,
        video_quartile_25 AS video_p25_watched,
        video_quartile_50 AS video_p50_watched,
        video_quartile_75 AS video_p75_watched,
        video_quartile_100 AS video_completions

        -- Excluded fields --
        /*
        conv,
        total_revenue,
        conversions,
        conversion_value,
        view_through_conv,
        conversion_rate,
        search_imp_share,
        search_rank_lost_impression_share,
        */

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'ad_group_id', 'keyword_id', 'ad_network_type']) }} AS id,
        'Google Ads' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final