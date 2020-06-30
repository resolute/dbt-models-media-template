WITH

data AS (
  
    SELECT * FROM {{ ref('stg_linkedin_ads__creatives') }}

),
  
extract_utm_params AS (

    SELECT
    
        *,
        REGEXP_EXTRACT(destination_url,r'[?&]utm_source=([^&]+)') as utm_source,
        REGEXP_EXTRACT(destination_url,r'[?&]utm_medium=([^&]+)') as utm_medium,
        REGEXP_EXTRACT(destination_url,r'[?&]utm_campaign=([^&]+)') as utm_campaign,
        REGEXP_EXTRACT(destination_url,r'[?&]utm_content=([^&]+)') as utm_content,
        REGEXP_EXTRACT(destination_url,r'[?&]utm_term=([^&]+)') as utm_term
    
    FROM data
    
),
  
general_definitions AS (

    SELECT
    
        *,
        'LinkedIn Paid' AS data_source,
        'LinkedIn' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Social' AS channel_name
  
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
        campaign_id,
        campaign_name,
        campaign_type,
        campaign_group_id,
        campaign_group_name,
        creative_id,
        ad_creative_name,
        ad_creative_text,
        ad_creative_title,
        creative_status,
        destination_url,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        video_duration,
        
        impres AS impressions,
        click AS link_clicks,
        action_clicks,
        ad_unit_clicks,
        text_url_clicks,
        spent AS cost,
        spend_local_currency,
        daily_budget,
        total_budget,
        total_engagements,
        general_definitions.like AS likes,
        comment,
        share,
        follow,
        video_views,
        video_first_quartile_completions,
        video_midpoint_completions,
        video_third_quartile_completions,
        video_completions,
        external_website_conversions,
        post_click_conv,
        post_view_conv,
        one_click_lead_form_opens,
        one_click_leads,
        viral_impres,
        viral_click,
        viral_like,
        viral_comment,
        viral_share,
        viral_follow
        
     FROM general_definitions

),

-- Find post age in days
final AS (

    SELECT
        
        *
    
    FROM rename_columns_and_set_defaults

)
  
SELECT * FROM final