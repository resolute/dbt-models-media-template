{%- set source_account_ids = var('pinterest_ads_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'pinterest_ads_pins_1v_30en_30cl') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

recast AS (

    SELECT *

        REPLACE((spend / 1000) AS spend)

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'pin_promotion_id']) }} AS id,
        *
    
    FROM recast

)

SELECT * FROM final