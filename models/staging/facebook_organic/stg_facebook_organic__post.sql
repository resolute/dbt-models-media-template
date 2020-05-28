WITH

source_data as (

    SELECT * FROM {{ source('facebook_organic', 'view_facebook_pages_post') }}

),

recast AS (
  
    SELECT 

        *
        
        REPLACE (CAST(created_time AS DATE) AS created_time)
    
    FROM source_data

    WHERE account_id = '374809010319'

),

rank_duplicate_post_ids AS (

    SELECT
        
        *,
        ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY created_time DESC, permalink DESC) AS post_most_recent_data_rank
    
    FROM recast

),

final AS (

    SELECT 
    
        {{ dbt_utils.surrogate_key(['created_time', 'account_id', 'post_id']) }} AS id,
        * 
        
        EXCEPT(post_most_recent_data_rank)
    
    FROM rank_duplicate_post_ids
    
    WHERE post_most_recent_data_rank = 1

)

SELECT * FROM final