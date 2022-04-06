{%- set source_account_ids = var('linkedin_organic_ids') -%}

{{ config(enabled= (var('linkedin_organic_ids'))|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'linkedin_organic_followers_lifetime_inc') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final