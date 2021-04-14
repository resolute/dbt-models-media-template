{{ config(enabled= (var('youtube_organic_ids'))|length > 0 is true) }}

WITH

yt_videos AS (
  
    SELECT * FROM {{ ref('stg_youtube_organic__top_videos') }}

),
  
final AS (

    SELECT
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'video_id']) }} AS id,
        *

        EXCEPT(id)
        
     FROM yt_videos

)
      
SELECT * FROM final