WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'facebook_ads_creative') }}

    WHERE account_id IN UNNEST({{ var('facebook_ads_ids') }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'creative_id']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final