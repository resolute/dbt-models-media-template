WITH

source_data as (

    SELECT * FROM {{ source('facebook_organic', 'view_facebook_pages_page') }}

),

final AS (
  
    SELECT 
    
        *
    
    FROM source_data

    WHERE account_id = '374809010319'

)

SELECT * FROM final