WITH

source_data as (

    SELECT * FROM {{ source('improvado', 'linkedin_organic_followers_lifetime') }}

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id']) }} AS id,
        *
    
    FROM source_data

    WHERE account_id IN UNNEST({{ var('linkedin_organic_ids') }})

)

SELECT * FROM final