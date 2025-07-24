{%- set source_account_ids = get_account_ids('google ads') -%}

{# Identify whether to enable this type of Google Ads model #}
{%- set enable_google_ads_model = true -%}
{%- set ev_enable_models = fromyaml(env_var('DBT_GOOGLE_ADS_MODELS_ENABLED', '')) -%}
{%- if ev_enable_models is not none and 'campaign' not in ev_enable_models -%}
    {%- set enable_google_ads_model = false -%}
{%- elif var('google_ads_models_enabled', [])|length > 0 is true and 'campaign' not in var('google_ads_models_enabled', []) -%}
    {%- set enable_google_ads_model = false -%}
{%- endif -%}

{{ config(enabled= source_account_ids|length > 0 is true and enable_google_ads_model is true and get_account_conversion_data_config('google ads')) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_ads_campaign_device_conversions') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT
        
        {# Dimensions -#}
        account_id,
        account_name,
        date,
        customer_id,
        device,     
        campaign_id,
        campaign_name,
        conversion_name AS conversion_action_name,
        conversion_category AS conversion_action_category,
        conversion_tracker_id AS conversion_action_id,
        ad_network_type,

        {#- General metrics -#}
        all_conversions AS all_conv,
        all_conversions_value AS value_all_conv,
        all_conversions_by_conversion_date,
        all_conversions_value_by_conversion_date,
        conversions,
        conversions_value AS value_conversions,
        conversions_by_conversion_date,
        conversions_value_by_conversion_date,        
        view_through_conversions AS conversions_view_through,
        cross_device_conversions


    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'campaign_id', 'ad_network_type', 'conversion_action_id', 'device']) }} AS id,
        'Google Ads' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final