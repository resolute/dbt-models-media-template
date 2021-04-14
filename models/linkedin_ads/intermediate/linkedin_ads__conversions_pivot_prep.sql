{{ config(enabled=var('linkedin_ads_conversions_enabled')) }}

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