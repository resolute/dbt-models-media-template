{{-
    config(
        enabled = var('youtube_organic_ids') != None
    )
-}}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'youtube_organic_traffic_source_daily') }}

    WHERE account_id IN UNNEST({{ var('youtube_organic_ids') }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'traffic_source', 'subscribed_status']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final