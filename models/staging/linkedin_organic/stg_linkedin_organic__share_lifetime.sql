WITH

source_data as (

    SELECT * FROM {{ source('linkedin_organic', 'view_linkedin_organic_share_lifetime') }}

),

final AS (
  
    SELECT 
    
        *
    
    FROM source_data

    WHERE account_id = 'urn:li:organization:15248569'

)

SELECT * FROM final