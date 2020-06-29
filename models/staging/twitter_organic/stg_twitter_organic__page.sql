{{-
    config(
        enabled = var('twitter_organic_ids') != None
    )
-}}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'twitter_page') }}

    WHERE account_id IN UNNEST({{ var('twitter_organic_ids') }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final