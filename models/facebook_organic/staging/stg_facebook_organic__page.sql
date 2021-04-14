{%- set source_account_ids = var('facebook_organic_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'facebook_pages_page') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'page_id']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final