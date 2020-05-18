WITH

source_data as (

    SELECT * FROM {{ source('twitter_organic', 'view_twitter_organic_tweets') }}

),

final AS (
  
    SELECT 
    
        *
    
    FROM source_data

    WHERE account_id = '3e2bc'

)

SELECT * FROM final