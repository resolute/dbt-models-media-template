WITH

source_data as (

    SELECT * FROM {{ source('improvado', 'linkedin_organic_share_lifetime') }}

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'share']) }} AS id,
        *
    
    FROM source_data

    WHERE account_id IN UNNEST({{ var('linkedin_organic_ids') }})

)

SELECT * FROM final