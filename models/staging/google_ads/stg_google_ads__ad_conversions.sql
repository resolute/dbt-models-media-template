{%- set source_account_ids = var('google_ads_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_adwords_ad_conversions') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        account_id,
        account_name,
        date,
        customer_id,
        campaign_id,
        campaign_name,
        adset_id AS ad_group_id,
        adset_name AS ad_group_name,
        ad_id,
        ad_name,
        description,
        ad_network_type_1,
        headline_1 AS headline1,
        headline_2 AS headline2,
        creative_urls AS destination_url,
        criterion_id,
        device,
        conversion_category,
        conversion_name,
        conversion_tracker_id,
        
        {#- Conversions -#}
        conv AS all_conv,
        revenue AS value_all_conv,
        conversions AS conversions,
        conversion_value AS value_conversions,
        view_through_conv AS conversions_view_through

        -- Excluded fields --
        /*
        cross_device_conversions,
        leads_conversions,
        other_conversions,
        page_view_conversions,
        sales_conversions,
        sign_up_conversions,
        */

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_id', 'ad_group_id', 'ad_id', 'ad_network_type_1', 'device', 'criterion_id', 'conversion_tracker_id']) }} AS id,
        'Google Ads' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final