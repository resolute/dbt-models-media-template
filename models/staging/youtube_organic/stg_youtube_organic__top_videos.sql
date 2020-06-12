WITH

source_data as (

    SELECT * FROM {{ source('improvado', 'youtube_organic_top_videos') }}

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'video_id']) }} AS id,
        *
    
    FROM source_data

    WHERE account_id IN UNNEST({{ var('youtube_organic_ids') }})

)

SELECT * FROM final