WITH
  
final AS (
  
    SELECT 
    
        *
        
        REPLACE (CAST(created_time AS DATE) AS created_time)
    
    FROM {{ source('improvado', 'view_facebook_pages_post') }}
    
    WHERE account_id = '374809010319'

)

SELECT * FROM final