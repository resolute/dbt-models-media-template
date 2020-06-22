WITH

source_data as (

    SELECT * FROM {{ source('improvado', 'pinterest_ads_pins_1v_30en_30cl') }}

    WHERE account_id IN UNNEST({{ var('pinterest_ids') }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'pin_promotion_id']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final