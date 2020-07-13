{%- set source_account_ids = var('google_ads_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_adwords_ad_conversions') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_id', 'adset_id', 'ad_id', 'ad_network_type_1', 'device', 'criterion_id', 'conversion_tracker_id']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final