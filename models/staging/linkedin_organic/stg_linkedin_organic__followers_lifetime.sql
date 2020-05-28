WITH

source_data as (

    SELECT * FROM {{ source('linkedin_organic', 'view_linkedin_organic_followers_lifetime') }}

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id']) }} AS id,
        *
    
    FROM source_data

    WHERE account_id = 'urn:li:organization:15248569'

)

SELECT * FROM final