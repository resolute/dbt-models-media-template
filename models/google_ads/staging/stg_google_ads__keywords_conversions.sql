{{ config(enabled=var('google_ads_conversions_enabled')) }}

{%- set source_account_ids = var('google_ads_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_adwords_keywords_conversions') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        account_id,
        account_name,
        date,
        customer_id,
        account AS account_desc_name,
        campaign_id,
        campaign AS campaign_name,
        campaign_type,
        campaign_state,
        ad_group_id,
        ad_group_name,
        ad_group_state,
        keyword_id,
        keyword AS keyword_name,
        keyword_state,
        network AS ad_network_type_1,
        match_type,
        destination_url,
        label_ids,
        labels,
        conversion_category,
        conversion_name,
        conversion_tracker_id,
        conversion_source,
        
        {#- Conversions -#}
        all_conversions AS all_conv,
        all_conversions_value AS value_all_conv,
        conversions AS conversions,
        total_conversions_value AS value_conversions,
        view_through_conversions AS conversions_view_through

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'ad_group_id', 'keyword_id', 'ad_network_type_1', 'conversion_tracker_id']) }} AS id,
        'Google Ads' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final