{%- set source_account_ids = get_account_ids('google ads') -%}

{# Identify whether to enable this type of Google Ads model #}
{%- set enable_google_ads_model = true -%}
{%- set ev_enable_models = fromyaml(env_var('DBT_GOOGLE_ADS_MODELS_ENABLED', '')) -%}
{%- if ev_enable_models is not none and 'keywords' not in ev_enable_models -%}
    {%- set enable_google_ads_model = false -%}
{%- elif var('google_ads_models_enabled', [])|length > 0 is true and 'keywords' not in var('google_ads_models_enabled', []) -%}
    {%- set enable_google_ads_model = false -%}
{%- endif -%}

{{ config(enabled= source_account_ids|length > 0 is true and enable_google_ads_model is true and get_account_conversion_data_config('google ads')) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_ads_keywords_conversions') }}

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
        ad_group_label_ids,
        ad_group_labels,
        keyword_id,
        keyword_name,
        keyword_state AS keyword_status,
        match_type,
        ad_network_type,
        conversion_category AS conversion_action_category,
        conversion_name as conversion_action_name,
        conversion_id AS conversion_action_id,
        conversion_source AS external_conversion_source,
        
        {#- Conversions -#}
        conv AS all_conv,
        total_revenue AS value_all_conv,
        conversions,
        conversion_value AS value_conversions,
        view_through_conv AS conversions_view_through

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'ad_group_id', 'keyword_id', 'ad_network_type', 'conversion_action_id']) }} AS id,
        'Google Ads' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final