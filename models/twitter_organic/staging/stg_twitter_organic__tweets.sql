{%- set source_account_ids = var('twitter_organic_ids') -%}

{{ config(enabled= (var('twitter_organic_ids'))|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'twitter_organic_tweets') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'organic_tweet_id']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final