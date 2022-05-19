{%- set source_account_ids = get_account_ids('linkedin ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true and get_account_conversion_data_config('linkedin ads')) }}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_linkedin_ads__conversions') }}

),

-- Format the conversion dimensions to all lowercase and remove any non letter, number, or underscore.
final AS (

    SELECT
    
        *,
        REGEXP_REPLACE(REPLACE(LOWER(conversion_type), " ", "_"), r"([^a-zA-Z0-9_])", "") AS conversion_type_formatted,
        REGEXP_REPLACE(REPLACE(LOWER(conversion_name), " ", "_"), r"([^a-zA-Z0-9_])", "") AS conversion_name_formatted
    
    FROM data
    
)

SELECT * FROM final