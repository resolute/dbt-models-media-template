{%- set source_account_ids = get_account_ids('google campaign manager') -%}

{{ config(enabled= source_account_ids|length > 0 is true and get_account_conversion_data_config('google campaign manager')) }}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_google_campaign_manager__ads_creatives_conversions') }}

),

-- Format the conversion dimensions to all lowercase and remove any non letter, number, or underscore.
final AS (

    SELECT
    
        *,
        REGEXP_REPLACE(REPLACE(LOWER(activity_group), " ", "_"), r"([^a-zA-Z0-9_])", "") AS activity_group_formatted,
        REGEXP_REPLACE(REPLACE(LOWER(activity), " ", "_"), r"([^a-zA-Z0-9_])", "") AS activity_formatted
    
    FROM data
    
)

SELECT * FROM final