{%- set source_account_ids = var('facebook_ads_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'facebook_ads_creative') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

recast AS (

    SELECT *

        REPLACE(DATE(CAST(publication_date AS DATETIME)) AS publication_date)

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'creative_id']) }} AS id,
        *
    
    FROM recast

)

SELECT * FROM final