{%- set source_account_ids = get_account_ids('linkedin ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true and get_account_conversion_data_config('linkedin ads')) }}

{# Identify the conversion names to include in this model #}
{%- set conversion_names = [] -%}
{%- set ev_conversion_types = fromyaml(env_var('DBT_LINKEDIN_ADS_CONVERSION_NAMES', '')) -%}
{%- if ev_conversion_types is not none -%}
    {%- set conversion_names = ev_conversion_types -%}
{%- else -%}
    {%- set conversion_names = var('linkedin_ads_conversion_names', []) -%}
{%- endif -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_linkedin_ads__conversions') }}

),

-- Format the conversion dimensions to all lowercase and remove any non letter, number, or underscore. Also, filter for conversions names includes in the google_campaign_manager_conversion_names variable.
final AS (

    SELECT
    
        *,
        REGEXP_REPLACE(REPLACE(LOWER(conversion_type), " ", "_"), r"([^a-zA-Z0-9_])", "") AS conversion_type_formatted,
        REGEXP_REPLACE(REPLACE(LOWER(conversion_name), " ", "_"), r"([^a-zA-Z0-9_])", "") AS conversion_name_formatted
    
    FROM data

    {% if conversion_names|length > 0 is true -%}
    WHERE conversion_name IN UNNEST({{ conversion_names }})
    {%- endif %}
    
)

SELECT * FROM final