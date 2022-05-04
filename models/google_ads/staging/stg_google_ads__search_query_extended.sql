{%- set source_account_ids = get_account_ids('google ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_ads_ql_search_query_keywords') }}

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
        advertising_channel_type AS campaign_type,
        advertising_channel_sub_type AS campaign_sub_type,
        ad_group_id,
        ad_group_name,
        keyword_id,
        keyword_name,
        search_term,
        search_term_match_type,
        ad_network_type,
        
        {#- General metrics -#}
        impressions,
        cost,
        clicks AS link_clicks,
        engagements,
        interactions,

        -- Excluded fields --
        /*
        all_conversions,
        all_conversions_value,
        conversions,
        conversions_value,
        view_through_converions,
        */

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'ad_group_id', 'keyword_id', 'search_term', 'ad_network_type', 'search_term_match_type']) }} AS id,
        'Google Ads' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final