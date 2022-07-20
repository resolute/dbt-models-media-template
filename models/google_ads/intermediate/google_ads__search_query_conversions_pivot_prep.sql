{%- set source_account_ids = get_account_ids('google ads') -%}

{# Identify whether to enable this type of Google Ads model #}
{%- set enable_google_ads_model = true -%}
{%- set ev_enable_models = fromyaml(env_var('DBT_GOOGLE_ADS_MODELS_ENABLED', '')) -%}
{%- if ev_enable_models is not none and 'search query' not in ev_enable_models -%}
    {%- set enable_google_ads_model = false -%}
{%- elif var('google_ads_models_enabled', [])|length > 0 is true and 'search query' not in var('google_ads_models_enabled', []) -%}
    {%- set enable_google_ads_model = false -%}
{%- endif -%}

{{ config(enabled= source_account_ids|length > 0 is true and enable_google_ads_model is true and get_account_conversion_data_config('google ads')) }}

{# Identify the conversion names to include in this model #}
{%- set conversion_names = [] -%}
{%- set ev_conversion_types = fromyaml(env_var('DBT_GOOGLE_ADS_CONVERSION_NAMES', '')) -%}
{%- if ev_conversion_types is not none -%}
    {%- set conversion_names = ev_conversion_types -%}
{%- else -%}
    {%- set conversion_names = var('google_ads_conversion_names', []) -%}
{%- endif -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_google_ads__search_query_conversions') }}

),

-- Format the conversion dimensions to all lowercase and remove any non letter, number, or underscore. Also, filter for conversions names includes in the google_campaign_manager_conversion_names variable.
final AS (

    SELECT
    
        *,
        REGEXP_REPLACE(REPLACE(LOWER(conversion_action_category), " ", "_"), r"([^a-zA-Z0-9_])", "") AS conversion_action_category_formatted,
        REGEXP_REPLACE(REPLACE(LOWER(conversion_action_name), " ", "_"), r"([^a-zA-Z0-9_])", "") AS conversion_action_name_formatted
    
    FROM data

    {% if conversion_names|length > 0 is true -%}
    WHERE conversion_action_name IN UNNEST({{ conversion_names }})
    {%- endif %}
    
)

SELECT * FROM final