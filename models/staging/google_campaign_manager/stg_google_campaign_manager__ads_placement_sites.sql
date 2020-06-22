WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'dcm_ads_placement_sites') }}

    WHERE advertiser_id IN UNNEST({{ var('google_campaign_manager_ids') }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'advertiser_id', 'site_id_dcm', 'placement_id', 'ad_id', 'creative_id']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final