{%- set source_account_ids = var('google_ads_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_adwords_search_query_conversions') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT

        account_id,
        account_name,
        date,
        customer_id,
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        keyword_id,
        keyword AS keyword_name,
        network AS ad_network_type_1,
        search_term,
        match_type AS query_match_type,
        conv_category_name AS conversion_category,
        conv_type_name AS conversion_name,
        conv_tracker_id AS conversion_tracker_id,
        conversion_source,
        
        conv AS all_conversions,
        revenue AS all_conversion_value,
        conversions,
        conversion_value,
        view_through_conv

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_id', 'ad_group_id', 'keyword_id', 'search_term', 'query_match_type', 'ad_network_type_1', 'conversion_tracker_id']) }} AS id,
        'Google Ads' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final