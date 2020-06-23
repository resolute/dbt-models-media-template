WITH

ads_data AS (
  
    SELECT * FROM {{ ref('google_campaign_manager__ads_creatives_placements_daily') }}

),

conversion_data AS (
  
    SELECT * FROM {{ ref('google_campaign_manager__conversions_daily') }}

),

final AS (

    SELECT

        ads_data.*,
        conversion_data.* EXCEPT(id, account_id, date, advertiser_id, campaign_id, site_id, placement_id, ad_id, creative_id)

    FROM ads_data

    LEFT JOIN conversion_data USING (id)
)

SELECT * FROM final