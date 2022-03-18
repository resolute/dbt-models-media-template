{{ config(enabled= (var('linkedin_ads_ids'))|length > 0 is true) }}

{%- set source_account_ids = var('linkedin_ads_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'linkedin_ads_creatives') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        account_id,
        account_name,
        date,
        campaign_group_id,
        campaign_group_name,
        campaign_id,
        campaign_name,
        campaign_type,
        campaign_objective_type AS campaign_objective,
        creative_id,
        ad_creative_name AS creative_name,
        ad_creative_text AS creative_text,
        ad_creative_title AS creative_title,
        creative_status,
        creative_reference,
        destination_url,
        -- Remove any decimals and set null values to zero from video length
        CAST(
            ROUND(
                IFNULL(SAFE_CAST(video_duration AS FLOAT64), 0)
                ) 
            AS STRING) AS video_length,
        currency,
        
        {#- General metrics -#}
        approximate_unique_impressions AS impressions_unique,
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
        reactions,
        source_data.like AS likes,
        comment,
        share,
        follow,
        viral_impres AS viral_impressions,
        viral_click,
        viral_like,
        viral_comment,
        viral_share,
        viral_follow,

        {#- Video metrics -#}
        video_views,
        video_first_quartile_completions AS video_p25_watched,
        video_midpoint_completions AS video_p50_watched,
        video_third_quartile_completions AS video_p75_watched,
        video_completions,

        {#- Conversions -#}
        one_click_lead_form_opens AS conv_one_click_lead_form_opens,
        one_click_leads AS conv_one_click_leads

        -- Excluded fields --
        /*
        external_website_conversions,
        post_click_conv,
        post_view_conv,
        viral_conversions,
        viral_post_click_conversions,
        viral_post_view_conversions,
        */
        

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'creative_id']) }} AS id,
        'LinkedIn Paid' AS data_source,
        'LinkedIn' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Social' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final