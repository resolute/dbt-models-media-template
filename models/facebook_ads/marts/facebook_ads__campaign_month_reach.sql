WITH

data AS (
  
    SELECT * FROM {{ ref('stg_facebook_ads__campaign_month_reach') }}

),
  
final AS (

    SELECT
    
        *
        
     FROM data

)
      
SELECT * FROM final