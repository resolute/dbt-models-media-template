WITH

source_data as (

    SELECT * FROM {{ source('improvado', 'facebook_ads_creative') }}

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'creative_id']) }} AS id,
        *
    
    FROM source_data

    WHERE account_id IN UNNEST({{ var('facebook_ads_ids') }})

)

SELECT * FROM final