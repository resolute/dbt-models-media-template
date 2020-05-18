WITH

yt_organic AS (
  
    SELECT * FROM {{ ref('stg_youtube_organic__traffic_source_daily') }}

),
  
yt_organic_general_definitions AS (
    
    SELECT
    
        *,
        'YouTube Organic' AS data_source,
        'YouTube' AS channel_source_name,
        'Organic' AS channel_source_type,
        'Organic Video' AS channel_name
        
    FROM yt_organic

),

final AS (

    SELECT
    
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        date,
        channel_id,
        channel_title,
        
        CASE
            WHEN traffic_source = 'YT_SEARCH' THEN 'YouTube search'
            WHEN traffic_source = 'EXT_URL' THEN 'External'
            WHEN traffic_source = 'RELATED_VIDEO' THEN 'Suggested videos'
            WHEN traffic_source = 'SUBSCRIBER' THEN 'Browse features'
            WHEN traffic_source = 'NO_LINK_OTHER' THEN 'Direct or unknown'
            WHEN traffic_source = 'YT_OTHER_PAGE' THEN 'Other YouTube features'
            WHEN traffic_source = 'YT_CHANNEL' THEN 'Channel pages'
            WHEN traffic_source = 'PLAYLIST' THEN 'Playlists'
            WHEN traffic_source = 'YT_PLAYLIST_PAGE' THEN 'Playlist page'
            WHEN traffic_source = 'END_SCREEN' THEN 'End screens'
            WHEN traffic_source = 'NOTIFICATION' THEN 'Notifications'
            WHEN traffic_source = 'ANNOTATION' THEN 'Video cards and annotations'
        END AS traffic_source,
        
        subscribed_status,
        live_or_on_demand,
        estimated_minutes_watched,
        views
        
     FROM yt_organic_general_definitions

)
      
SELECT * FROM final