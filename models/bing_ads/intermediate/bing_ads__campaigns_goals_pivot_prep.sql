{%- set source_account_ids = get_account_ids('bing ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true and get_account_conversion_data_config('bing ads')) }}

{# Identify the conversion names to include in this model #}
{%- set conversion_names = [] -%}
{%- set ev_conversion_types = fromyaml(env_var('DBT_BING_ADS_CONVERSION_NAMES', '')) -%}
{%- if ev_conversion_types is not none -%}
    {%- set conversion_names = ev_conversion_types -%}
{%- else -%}
    {%- set conversion_names = var('bing_ads_conversion_names', []) -%}
{%- endif -%}


WITH

data AS (
  
    SELECT * FROM {{ ref('stg_bing_ads__campaign_goal') }}

),

-- Format the conversion dimensions to all lowercase and remove any non letter, number, or underscore.
final AS (

    SELECT
    
        *,
        REGEXP_REPLACE(REPLACE(LOWER(goal), " ", "_"), r"([^a-zA-Z0-9_])", "") AS goal_formatted
    
    FROM data
    
)

SELECT * FROM final