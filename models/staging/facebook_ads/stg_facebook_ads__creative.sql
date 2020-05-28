WITH

source_data as (

    SELECT * FROM {{ source('facebook_ads', 'view_facebook_ads_creative') }}

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'creative_id']) }} AS id,
        *
    
    FROM source_data

    WHERE account_id = 'act_317216715720714'

)

SELECT * FROM final