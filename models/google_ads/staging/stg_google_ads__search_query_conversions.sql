{%- set source_account_ids = get_account_ids('google ads') -%}

{{ config(enabled= get_account_conversion_data_config('google ads')) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_ads_search_query_conversions') }}

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
        ad_group_criterion_keyword_id AS keyword_id,
        keyword_info_text AS keyword_name,
        search_term,
        search_term_match_type,
        ad_network_type,
        conversion_action_category,
        conversion_action_name,
        conversion_action_id,
        external_conversion_source,
        
        {#- Conversions -#}
        all_conversions AS all_conv,
        all_conversions_value AS value_all_conv,
        conversions AS conversions,
        conversion_value AS value_conversions,
        view_through_conversions AS conversions_view_through

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'ad_group_id', 'keyword_id', 'search_term', 'ad_network_type', 'search_term_match_type', 'conversion_action_id']) }} AS id,
        'Google Ads' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final