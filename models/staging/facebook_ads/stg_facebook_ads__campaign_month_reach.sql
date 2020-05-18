WITH

source_data as (

    SELECT * FROM {{ source('facebook_ads', 'view_facebook_campaign_month_reach') }}

),

final AS (
  
    SELECT 
    
        *
    
    FROM source_data

    WHERE account_id = 'act_317216715720714'

)

SELECT * FROM final