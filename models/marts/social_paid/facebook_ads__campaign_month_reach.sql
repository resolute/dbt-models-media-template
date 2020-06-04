WITH

data AS (
  
    SELECT * FROM {{ ref('stg_facebook_ads__creative') }}

),
  
final AS (

    SELECT
    
        *
        
     FROM data

)
      
SELECT * FROM final