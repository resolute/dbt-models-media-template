{{ config(enabled= (var('facebook_ads_ids'))|length > 0 is true) }}

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