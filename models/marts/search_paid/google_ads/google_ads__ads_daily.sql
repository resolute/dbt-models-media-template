WITH

data AS (
  
    SELECT * FROM {{ ref('stg_google_ads__ads') }}

),
  
extract_utm_params AS (

    SELECT
    
        *,
        REGEXP_EXTRACT(final_url,r'[?&]utm_source=([^&]+)') as utm_source,
        REGEXP_EXTRACT(final_url,r'[?&]utm_medium=([^&]+)') as utm_medium,
        REGEXP_EXTRACT(final_url,r'[?&]utm_campaign=([^&]+)') as utm_campaign,
        REGEXP_EXTRACT(final_url,r'[?&]utm_content=([^&]+)') as utm_content,
        REGEXP_EXTRACT(final_url,r'[?&]utm_term=([^&]+)') as utm_term
    
    FROM data
    
),
  
general_definitions AS (

    SELECT
    
        *,
        'Google Ads' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name
  
    FROM extract_utm_params
    
),

rename_columns_and_set_defaults AS (

    SELECT
    
        id,
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        date,
        customer_id,
        account_desc_name,
        campaign_id,
        campaign_name,
        campaign_type,
        campaign_state,
        adset_id,
        adset_name,
        adgroup_state,
        ad_id,
        ad_name,
        ad_type,
        ad_state,
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
        ad_network_type_1,
        ad_network_type_2,
        label_ids,
        labels,
        avg_pos,
        gmail_ad_business_name,
        gmail_ad_headline,
        gmail_ad_description,
        gmail_ad_marketing_image_headline,
        gmail_ad_marketing_img_desc,
        device_preference,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        
        imps AS impressions,
        clicks AS link_clicks,
        spend AS cost,
        engagements,
        interactions,
        gmail_clicks_to_website,
        views,
        video_quartile_25,
        video_quartile_50,
        video_quartile_75,
        video_quartile_100,
        conv,
        conversions,
        view_through_conv,
        conversion_value,
        conversion_rate,
        purchase,
        revenue
        
     FROM general_definitions

),

-- Find post age in days
final AS (

    SELECT
        
        *
    
    FROM rename_columns_and_set_defaults

)
  
SELECT * FROM final