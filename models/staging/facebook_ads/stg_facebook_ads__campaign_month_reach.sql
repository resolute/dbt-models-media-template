WITH

source_data as (

    SELECT * FROM {{ source('improvado', 'facebook_campaign_month_reach') }}

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_id']) }} AS id,
        *
    
    FROM source_data

    WHERE account_id IN UNNEST({{ var('facebook_ads_ids') }})

)

SELECT * FROM final