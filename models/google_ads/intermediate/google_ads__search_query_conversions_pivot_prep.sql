{{ config(enabled= get_account_conversion_data_config('google ads')) }}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_google_ads__search_query_conversions') }}

),

-- Format the conversion dimensions to all lowercase and remove any non letter, number, or underscore.
final AS (

    SELECT
    
        *,
        REGEXP_REPLACE(REPLACE(LOWER(conversion_action_category), " ", "_"), r"([^a-zA-Z0-9_])", "") AS conversion_action_category_formatted,
        REGEXP_REPLACE(REPLACE(LOWER(conversion_action_name), " ", "_"), r"([^a-zA-Z0-9_])", "") AS conversion_action_name_formatted
    
    FROM data
    
)

SELECT * FROM final