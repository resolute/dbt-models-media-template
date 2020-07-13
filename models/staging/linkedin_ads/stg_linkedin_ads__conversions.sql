{%- set source_account_ids = var('linkedin_ads_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'linkedin_ads_conversions') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_id', 'conversion_id']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final