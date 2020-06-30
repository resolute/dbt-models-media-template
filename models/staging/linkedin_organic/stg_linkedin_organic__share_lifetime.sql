{{-
    config(
        enabled = var('linkedin_organic_ids') != None
    )
-}}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'linkedin_organic_share_lifetime') }}

    WHERE account_id IN UNNEST({{ var('linkedin_organic_ids') }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'share']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final