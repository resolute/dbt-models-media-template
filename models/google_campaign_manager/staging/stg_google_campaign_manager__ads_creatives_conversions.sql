{%- set source_account_ids = get_account_ids('google campaign manager') -%}

{{ config(enabled= get_account_conversion_data_config('google campaign manager')) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_cm_ads_creatives_placements') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        account_id,
        account_name,
        date,
        advertiser_id,
        advertiser_name,
        campaign_id,
        campaign_name,
        site_id,
        site,
        placement_id,
        placement,
        ad_id,
        ad,
        creative_id,
        creative,
        ad_type,
        creative_pixel_size,
        placement_size,
        activity_group_id,
        activity_group,
        activity_id,
        activity,

        {#- Conversions -#}
        total_conversions AS conversions,
        click_through_conversions AS conversions_click_through,
        view_through_conversions AS conversions_view_through,
        total_conversions_revenue AS value_conversions,
        click_through_revenue AS value_conversions_click_through,
        view_through_revenue AS value_conversions_view_through
        

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'advertiser_id', 'site_id', 'placement_id', 'placement_size', 'ad_id', 'creative_id', 'activity_id']) }} AS id,
        'Campaign Manager' AS data_source,
        'Campaign Manager' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Display' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final