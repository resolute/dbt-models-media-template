{%- set source_account_ids = get_account_ids('facebook organic') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'facebook_pages_post') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

recast AS (
  
    SELECT 

        *
        
        REPLACE (
            CAST(created_time AS DATE) AS created_time,
            DATETIME(TIMESTAMP(created_at_datetime)) AS created_at_datetime
        )
    
    FROM source_data

),

rank_duplicate_post_ids AS (

    SELECT
        
        *,
        ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY created_time DESC, permalink DESC) AS post_most_recent_data_rank
    
    FROM recast

),

final AS (

    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['created_time', 'account_id', 'post_id']) }} AS id,
        * 
        
        EXCEPT(post_most_recent_data_rank)
    
    FROM rank_duplicate_post_ids
    
    WHERE post_most_recent_data_rank = 1

)

SELECT * FROM final