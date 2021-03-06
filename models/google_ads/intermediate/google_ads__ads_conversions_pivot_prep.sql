{{ config(enabled=var('google_ads_conversions_enabled')) }}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_google_ads__ad_conversions') }}

),

-- Format the conversion dimensions to all lowercase and remove any non letter, number, or underscore.
final AS (

    SELECT
    
        *,
        REGEXP_REPLACE(REPLACE(LOWER(conversion_category), " ", "_"), r"([^a-zA-Z0-9_])", "") AS conversion_category_formatted,
        REGEXP_REPLACE(REPLACE(LOWER(conversion_name), " ", "_"), r"([^a-zA-Z0-9_])", "") AS conversion_name_formatted
    
    FROM data
    
)

SELECT * FROM final