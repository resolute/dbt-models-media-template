{%- set source_account_ids = var('google_ads_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_adwords_ads') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT
        
        {# Dimensions -#}
        account_id,
        account_name,
        date,
        customer_id,
        account_desc_name,
        campaign_id,
        campaign_name,
        campaign_type,
        campaign_state,
        adset_id AS ad_group_id,
        adset_name AS ad_group_name,
        adgroup_state AS ad_group_state,
        ad_id,
        ad_name,
        ad_type,
        ad_state,
        ad_network_type_1,
        ad_network_type_2,
        approval_status,
        image_ad_name,
        description,
        description_line1,
        description_line2,
        headline1,
        headline2,
        path1,
        path2,
        short_headline,
        long_headline,
        image_id,
        creative_image_height,
        creative_image_width,
        logo_id,
        display_url,
        final_url,
        final_url_as_link,
        mobile_final_url,
        image_url,
        image_url_as_ad_preview_url,
        tracking_template_url,
        destination_url,
        label_ids,
        labels,
        avg_pos,
        gmail_ad_business_name,
        gmail_ad_headline,
        gmail_ad_description,
        gmail_ad_marketing_image_headline,
        gmail_ad_marketing_img_desc,
        device_preference,

        {#- General metrics -#}
        imps AS impressions,
        spend AS cost,
        clicks AS link_clicks,
        engagements,
        interactions,
        gmail_clicks_to_website,

        {#- Video metrics -#}
        views AS video_views,
        video_quartile_25 AS video_p25_watched,
        video_quartile_50 AS video_p50_watched,
        video_quartile_75 AS video_p75_watched,
        video_quartile_100 AS video_completions

        -- Excluded fields --
        /*
        conv,
        revenue,
        conversions,
        conversion_value,
        view_through_conv,
        conversion_rate,
        purchase,
        conversions_donate,
        all_conv_donate,
        conversions_20_mile_conversion,
        all_conv_20_mile_conversion,
        conversions_conversion,
        all_conv_conversion,
        conversions_individual_registration,
        all_conv_individual_registration,
        */

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_id', 'ad_group_id', 'ad_id', 'ad_network_type_1', 'ad_network_type_2']) }} AS id,
        'Google Ads' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final