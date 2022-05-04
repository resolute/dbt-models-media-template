{{ config(enabled=var('google_ads_conversions_enabled')) }}

{%- set source_account_ids = var('google_ads_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_ads_ql_ads_device_conversions') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        account_id,
        account_name,
        date,
        campaign_id,
        campaign_name,
        campaign_advertisingchanneltype AS campaign_type,
        campaign_advertisingchannelsubtype AS campaign_sub_type,
        campaign_status,
        adgroup_id AS ad_group_id,
        adgroup_name AS ad_group_name,
        adgroup_status AS ad_group_status,
        ad_id,
        ad_name,
        ad_type,
        ad_network_type,
        device,
        conversion_action_category,
        conversion_action_name,
        conversion_action AS conversion_action_id,
        external_conversion_source,
        
        {#- Conversions -#}
        all_conversions AS all_conv,
        all_conversions_value AS value_all_conv,
        conversions,
        conversions_value AS value_conversions,
        view_through_conversions AS conversions_view_through

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'ad_id', 'ad_network_type', 'device', 'conversion_action_id']) }} AS id,
        'Google Ads' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final