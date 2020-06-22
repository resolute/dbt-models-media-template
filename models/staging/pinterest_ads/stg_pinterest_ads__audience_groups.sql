WITH

source_data as (

    SELECT * FROM {{ source('improvado', 'pinterest_ads_audience_groups') }}

    WHERE account_id IN UNNEST({{ var('pinterest_ids') }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'scope', 'type', 'dimension_value']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final