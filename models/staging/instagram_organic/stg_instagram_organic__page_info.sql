WITH

source_data as (

    SELECT * FROM {{ source('improvado', 'instagram_organic_page_info') }}

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id']) }} AS id,
        *
    
    FROM source_data

    WHERE account_id IN UNNEST({{ var('instagram_organic_ids') }})

)

SELECT * FROM final