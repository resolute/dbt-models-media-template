WITH

source_data as (

    SELECT * FROM {{ source('improvado', 'twitter_organic_tweets') }}

    WHERE account_id IN UNNEST({{ var('twitter_ids') }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'organic_tweet_id']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final