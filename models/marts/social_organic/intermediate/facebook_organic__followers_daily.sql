WITH

data AS (
  
    SELECT * FROM {{ ref('stg_facebook_organic__page') }}

),
  
general_definitions AS (
    
    SELECT
    
        *,
        'Facebook Organic' AS data_source,
        'Facebook' AS channel_source_name,
        'Organic' AS channel_source_type,
        'Organic Social' AS channel_name
        
    FROM data

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
        
        page_fans AS social_followers_total,
        page_fans - LAG(page_fans) OVER (PARTITION BY account_id ORDER BY date ASC) AS social_followers_net
        
     FROM general_definitions

)
      
SELECT * FROM final