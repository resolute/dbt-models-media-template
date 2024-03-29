{{ config(enabled= get_account_ids('instagram organic')|length > 0 is true) }}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_instagram_organic__page_info') }}

),
  
general_definitions AS (
    
    SELECT
    
        *,
        'Instagram Organic' AS data_source,
        'Instagram' AS channel_source_name,
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
        
        followers_count AS social_followers_total,
        followers_count - LAG(followers_count) OVER (PARTITION BY account_id ORDER BY date ASC) AS social_followers_net
        
     FROM general_definitions

)
      
SELECT * FROM final
