{%- set source_account_ids = get_account_ids('google campaign manager') -%}

{{ config(enabled= source_account_ids|length > 0 is true and get_account_conversion_data_config('google campaign manager')) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_cm_ads_creatives_placements') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

gcm_entity_creative_data AS (

    SELECT * FROM {{ ref('stg_google_campaign_manager__entity_creatives') }}

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        source_data.account_id,
        source_data.account_name,
        source_data.date,
        source_data.advertiser_id,
        source_data.advertiser_name,
        source_data.campaign_id,
        source_data.campaign_name,
        source_data.site_id,
        source_data.site,
        source_data.placement_id,
        source_data.placement,
        source_data.ad_id,
        source_data.ad,
        source_data.creative_id,
        COALESCE(creative.creative, source_data.creative) AS creative,
        source_data.ad_type,
        creative.creative_pixel_size,
        source_data.placement_size,
        source_data.activity_group_id,
        source_data.activity_group,
        source_data.activity_id,
        source_data.activity,

        {#- Conversions -#}
        total_conversions AS conversions,
        click_through_conversions AS conversions_click_through,
        view_through_conversions AS conversions_view_through,
        total_conversions_revenue AS value_conversions,
        click_through_revenue AS value_conversions_click_through,
        view_through_revenue AS value_conversions_view_through
        

    FROM source_data

    LEFT JOIN gcm_entity_creative_data AS creative
        ON source_data.creative_id = creative.creative_id

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'advertiser_id', 'site_id', 'placement_id', 'placement_size', 'ad_id', 'creative_id', 'activity_id']) }} AS id,
        'Campaign Manager' AS data_source,
        'Campaign Manager' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Display' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final