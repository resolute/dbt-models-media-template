WITH

source_data as (

    SELECT * FROM {{ source('instagram_organic', 'view_instagram_organic_post') }}

),

final AS (
  
    SELECT 
    
        *
    
    FROM source_data

    WHERE account_id = '17841401127220026'

)

SELECT * FROM final