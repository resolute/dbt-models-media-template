WITH

source_data as (

    SELECT * FROM {{ source('facebook_organic', 'view_facebook_pages_post') }}

),

final AS (
  
    SELECT 
    
        *
        
        REPLACE (CAST(created_time AS DATE) AS created_time)
    
    FROM source_data

    WHERE account_id = '374809010319'

)

SELECT * FROM final