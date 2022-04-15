{%- set source_account_ids = get_account_ids('google ads') -%}

{{ config(enabled= get_account_conversion_data_config('google ads')) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_adwords_search_query_conversions') }}

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
        
        {#- Conversions -#}
        conv AS all_conv,
        revenue AS value_all_conv,
        conversions AS conversions,
        conversion_value AS value_conversions,
        view_through_conv AS conversions_view_through

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'ad_group_id', 'keyword_id', 'ad_network_type_1', 'search_term', 'query_match_type', 'conversion_tracker_id']) }} AS id,
        'Google Ads' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final