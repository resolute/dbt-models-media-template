WITH

source_data as (

    SELECT * FROM {{ source('instagram_organic', 'view_instagram_organic_page_info') }}

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id']) }} AS id,
        *
    
    FROM source_data

    WHERE account_id = '17841401127220026'

)

SELECT * FROM final