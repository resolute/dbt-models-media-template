{%- set source_account_ids = get_account_ids('google ads') -%}

{# Identify whether to enable this type of Google Ads model #}
{%- set enable_google_ads_model = true -%}
{%- set ev_enable_models = fromyaml(env_var('DBT_GOOGLE_ADS_MODELS_ENABLED', '')) -%}
{%- if ev_enable_models is not none and 'keywords' not in ev_enable_models -%}
    {%- set enable_google_ads_model = false -%}
{%- elif var('google_ads_models_enabled', [])|length > 0 is true and 'keywords' not in var('google_ads_models_enabled', []) -%}
    {%- set enable_google_ads_model = false -%}
{%- endif -%}

{{ config(enabled= source_account_ids|length > 0 is true and enable_google_ads_model is true) }}

WITH

aggregate AS (

{{
    aggregate_model(ref('stg_google_ads__keywords_extended'), 'SUM', ['id'])
}}

),

final AS (

    SELECT
        
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'ad_group_id', 'keyword_id', 'ad_network_type']) }} AS id,
        *
    
    FROM aggregate

)

SELECT * FROM final