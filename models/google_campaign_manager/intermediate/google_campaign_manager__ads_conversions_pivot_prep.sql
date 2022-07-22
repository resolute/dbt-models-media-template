{%- set source_account_ids = get_account_ids('google campaign manager') -%}

{{ config(enabled= source_account_ids|length > 0 is true and get_account_conversion_data_config('google campaign manager')) }}

{# Identify the conversion names to include in this model #}
{%- set conversion_names = [] -%}
{%- set ev_conversion_types = fromyaml(env_var('DBT_GOOGLE_CAMPAIGN_MANAGER_CONVERSION_NAMES', '')) -%}
{%- if ev_conversion_types is not none -%}
    {%- set conversion_names = ev_conversion_types -%}
{%- else -%}
    {%- set conversion_names = var('google_campaign_manager_conversion_names', []) -%}
{%- endif -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_google_campaign_manager__ads_creatives_conversions') }}

),

-- Format the conversion dimensions to all lowercase and remove any non letter, number, or underscore. Also, filter for conversions names includes in the google_campaign_manager_conversion_names variable.
final AS (

    SELECT
    
        *,
        REGEXP_REPLACE(REPLACE(LOWER(activity_group), " ", "_"), r"([^a-zA-Z0-9_])", "") AS activity_group_formatted,
        REGEXP_REPLACE(REPLACE(LOWER(activity), " ", "_"), r"([^a-zA-Z0-9_])", "") AS activity_formatted
    
    FROM data

    {% if conversion_names|length > 0 is true -%}
    WHERE activity IN UNNEST({{ conversion_names }})
    {%- endif %}
    
)

SELECT * FROM final