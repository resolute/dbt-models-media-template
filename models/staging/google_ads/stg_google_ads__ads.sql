WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_adwords_ads') }}

    WHERE account_id IN UNNEST({{ var('google_ads_ids') }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_id', 'adset_id', 'ad_id', 'ad_network_type_1', 'ad_network_type_2']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final