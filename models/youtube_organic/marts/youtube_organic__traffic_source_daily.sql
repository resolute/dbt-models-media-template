{{ config(enabled= (var('youtube_organic_ids'))|length > 0 is true) }}

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
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'traffic_source', 'subscribed_status', 'live_or_on_demand']) }} AS id,
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        date,
        channel_id,
        channel_title,
        traffic_source,
        subscribed_status,
        live_or_on_demand,
        estimated_minutes_watched,
        views
        
     FROM yt_organic_general_definitions

)
      
SELECT * FROM final