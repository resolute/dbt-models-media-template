WITH

yt_videos AS (
  
    SELECT * FROM {{ ref('stg_youtube_organic__top_videos') }}

),
  
final AS (

    SELECT
    
        *
        
     FROM yt_videos

)
      
SELECT * FROM final