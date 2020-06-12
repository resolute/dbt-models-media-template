WITH

source_data as (

    SELECT * FROM {{ source('improvado', 'facebook_pages_page') }}

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'page_id']) }} AS id,
        *
    
    FROM source_data

    WHERE account_id IN {{ var('facebook_organic_ids') }}

)

SELECT * FROM final