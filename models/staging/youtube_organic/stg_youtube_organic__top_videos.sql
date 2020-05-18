WITH

source_data as (

    SELECT * FROM {{ source('youtube_organic', 'view_youtube_organic_top_videos') }}

),

final AS (
  
    SELECT 
    
        *
    
    FROM source_data

    WHERE account_id = 'UCGF_z7NNup6feP3ry_mRxnA'

)

SELECT * FROM final