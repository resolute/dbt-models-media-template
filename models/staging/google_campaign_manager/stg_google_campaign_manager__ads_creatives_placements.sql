WITH

source_data as (

    SELECT * FROM {{ source('improvado', 'dcm_ads_creatives_placements') }}

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'advertiser_id', 'site_id', 'placement_id', 'ad_id', 'creative_id', 'activity_id']) }} AS id,
        *
    
    FROM source_data

    WHERE advertiser_id IN UNNEST({{ var('google_campaign_manager_ids') }})

)

SELECT * FROM final