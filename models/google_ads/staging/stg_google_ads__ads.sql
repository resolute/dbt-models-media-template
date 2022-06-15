{%- set source_account_ids = var('google_ads_ids') -%}

{{ config(enabled= (var('google_ads_ids'))|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_ads_ads') }}

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
        campaign_advertising_channel_type AS campaign_type,
        campaign_status,
        campaign_label_ids,
        campaign_label_names AS campaign_labels,
        ad_group_id,
        ad_group_name,
        ad_group_status,
        ad_id,
        ad_name,
        ad_type,
        ad_status,
        ad_group_ad_label_ids AS ad_label_ids,
        ad_group_ad_labels AS ad_labels,
        ad_policy_summary_approval_status,
        ad_tracking_template_url,
        ad_display_url,
        ad_final_url,
        ad_final_url_as_link,
        ad_mobile_final_url,
        ad_device_preference,
        description,
        description_line1,
        description_line2,
        expanded_text_ad_headline1,
        expanded_text_ad_headline2,
        expanded_text_ad_headline3,
        expanded_text_ad_path1,
        expanded_text_ad_path2,
        responsive_display_ad_short_headline,
        responsive_display_ad_long_headline,
        responsive_display_ad_marketing_image_id,
        responsive_display_ad_square_logo_image_id,
        image_ad_name,
        image_ad_image_url,
        image_ad_pixel_height,
        image_ad_pixel_width,
        gmail_ad_teaser_business_name,
        gmail_ad_teaser_headline,
        gmail_ad_teaser_description,
        gmail_ad_marketing_image_headline,
        gmail_ad_marketing_image_description,
        ad_network_type,
        currency_code,

        {#- General metrics -#}
        impressions,
        cost,
        clicks AS link_clicks,
        engagements,
        interactions,
        gmail_secondary_clicks,

        {#- Video metrics -#}
        video_views,
        video_quartile_25 AS video_p25_watched,
        video_quartile_50 AS video_p50_watched,
        video_quartile_75 AS video_p75_watched,
        video_quartile_100 AS video_completions

        -- Excluded fields --
        /*
        all_conversions,
        all_conversions_value,
        conversions,
        conversions_value,
        */

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'ad_id', 'ad_network_type']) }} AS id,
        'Google Ads' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final