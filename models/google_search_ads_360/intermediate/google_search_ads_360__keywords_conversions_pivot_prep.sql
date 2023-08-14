{%- set source_account_ids = get_account_ids('google search ads 360') -%}

{{ config(enabled= source_account_ids|length > 0 is true and get_account_conversion_data_config('google search ads 360')) }}

{# Identify the conversion names to include in this model #}
{%- set conversion_names = [] -%}
{%- set ev_conversion_types = fromyaml(env_var('DBT_GOOGLE_SEARCH_ADS_360_CONVERSION_NAMES', '')) -%}
{%- if ev_conversion_types is not none -%}
    {%- set conversion_names = ev_conversion_types -%}
{%- else -%}
    {%- set conversion_names = var('google_search_ads_360_conversion_names', []) -%}
{%- endif -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_google_search_ads_360__keywords_conversion') }}

),

-- Format the conversion dimensions to all lowercase and remove any non letter, number, or underscore.
final AS (

    SELECT
    
        *,
        REGEXP_REPLACE(REPLACE(LOWER(floodlight_group), " ", "_"), r"([^a-zA-Z0-9_])", "") AS floodlight_group_formatted,
        REGEXP_REPLACE(REPLACE(LOWER(floodlight_activity), " ", "_"), r"([^a-zA-Z0-9_])", "") AS floodlight_activity_formatted
    
    FROM data
    
)

SELECT * FROM final