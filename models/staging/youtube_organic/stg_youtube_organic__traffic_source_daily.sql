WITH

source_data as (

    SELECT * FROM {{ source('youtube_organic', 'view_youtube_organic_traffic_source_daily') }}

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'traffic_source', 'subscribed_status']) }} AS id,
        *
    
    FROM source_data

    WHERE account_id = 'UCGF_z7NNup6feP3ry_mRxnA'

)

SELECT * FROM final