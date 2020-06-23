WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'youtube_organic_top_videos') }}

    WHERE account_id IN UNNEST({{ var('youtube_organic_ids') }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'video_id']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final