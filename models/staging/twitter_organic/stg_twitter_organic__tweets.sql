WITH

source_data as (

    SELECT * FROM {{ source('twitter_organic', 'view_twitter_organic_tweets') }}

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'organic_tweet_id']) }} AS id,
        *
    
    FROM source_data

    WHERE account_id = '3e2bc'

)

SELECT * FROM final