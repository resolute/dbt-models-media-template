{{ config(enabled= (var('linkedin_organic_ids'))|length > 0 is true) }}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_linkedin_organic__followers_lifetime') }}

),
  
general_definitions AS (
    
    SELECT
    
        *,
        'LinkedIn Organic' AS data_source,
        'LinkedIn' AS channel_source_name,
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
        
        total_followers_count AS social_followers_total,
        total_followers_count - LAG(total_followers_count) OVER (PARTITION BY account_id ORDER BY date ASC) AS social_followers_net
        
     FROM general_definitions

)
      
SELECT * FROM final