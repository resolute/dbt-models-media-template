{%- set source_account_ids = var('facebook_ads_ids') -%}

{{ config(enabled= (var('facebook_ads_ids'))|length > 0 is true) }}

{# Get a list of the columns from the upstream model #}
{%- set cols = adapter.get_columns_in_relation(source('improvado', 'facebook_ads_creative')) -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'facebook_ads_creative') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

facebook_entity_ad_data AS (

    SELECT * FROM {{ ref('stg_facebook_ads__entity_ads') }}

),

facebook_entity_adset_data AS (

    SELECT * FROM {{ ref('stg_facebook_ads__entity_adsets') }}

),

facebook_entity_campaign_data AS (

    SELECT * FROM {{ ref('stg_facebook_ads__entity_campaigns') }}

),

facebook_entity_creative_data AS (

    SELECT * FROM {{ ref('stg_facebook_ads__entity_creatives') }}

),

facebook_entity_account_data AS (

    SELECT * FROM {{ ref('stg_facebook_ads__entity_accounts') }}

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        source_data.account_id,
        facebook_entity_account_data.account_name AS account_name,
        source_data.account_name AS account_name_on_date,
        source_data.date,
        source_data.campaign_id,
        facebook_entity_campaign_data.campaign_name AS campaign_name,
        source_data.campaign_name AS campaign_name_on_date,
        source_data.campaign_type,
        source_data.adset_id,
        facebook_entity_adset_data.adset_name AS adset_name,
        source_data.adset_name AS adset_name_on_date,
        source_data.ad_id,
        facebook_entity_ad_data.ad_name AS ad_name,
        source_data.ad_name AS ad_name_on_date,
        source_data.creative_id,
        facebook_entity_creative_data.creative_name AS creative_name,
        source_data.creative_name AS creative_name_on_date,
        source_data.objective AS ad_objective,
        DATE(CAST(source_data.publication_date AS DATETIME)) AS ad_publication_date,
        source_data.object_type AS ad_type,
        source_data.body,
        source_data.name,
        source_data.description,
        source_data.caption,
        source_data.call_to_action_type,
        source_data.format_option,
        source_data.preview_shareable_link,
        source_data.instagram_permalink_url,
        source_data.creative_link,
        source_data.image,
        source_data.image_url,
        source_data.website_destination_url,
        source_data.creative_destination_url,
        source_data.video_creative_destination_url,
        source_data.buying_type,
        source_data.lead_gen_form_id,
        source_data.object_story_id,
        source_data.effective_object_story_id,
        source_data.effective_status,
        
        {#- General metrics -#}
        reach,
        impressions,
        spend AS cost,
        clicks,
        outbound_clicks,
        all_clicks,
        inline_link_clicks,
        unique_clicks,
        unique_link_click,

        {#- Engagement metrics -#}
        post_engagement AS post_engagement_total,
        inline_post_engagement,
        post_reactions,
        likes AS post_likes,
        comments AS post_comments,
        shares AS post_shares,
        onsite_post_save AS post_saves,
        post_story AS post_story_total,
        page_engagement AS page_engagement_total,
        page_story AS page_story_total,
        page_likes,
        app_engagement AS app_engagement_total,
        app_story AS app_story_total,
        app_use,
        mobile_app_install,
        instagram_profile_engagement AS instagram_profile_engagement_total,

        {#- Video metrics -#}
        video_view_3s,
        views AS video_views,
        video_play_actions_view_value,
        video_p25_watched_actions AS video_p25_watched,
        video_p50_watched_actions AS video_p50_watched,
        video_p75_watched_actions AS video_p75_watched,
        video_p95_watched_actions AS video_p95_watched,
        video_p100_watched_actions AS video_completions,
        thru_play AS video_thru_play,
        video_30_sec_watched_actions,
        video_avg_time_watched_actions,
        average_play_time_count AS video_average_play_time_count,

        {#- Conversions -#}
        add_payment_info AS conv_fb_add_payment_info_28c_1v,
        fb_mobile_add_payment_info AS conv_fb_mobile_add_payment_info_28c_1v,
        add_to_cart AS conv_fb_add_to_cart_28c_1v,
        add_to_wishlist AS conv_fb_add_to_wishlist_28c_1v,
        complete_registration AS conv_fb_complete_registration_28c_1v,
        fb_mobile_complete_registration AS conv_fb_mobile_complete_registration_28c_1v,
        donate_total AS conv_fb_donate_28c_1v,
        donate_total_value AS conv_fb_value_donate_28c_1v,
        initiate_checkout AS conv_fb_initiate_checkout_28c_1v,
        landing_page_view AS conv_fb_landing_page_view_28c_1v,
        lead_grouped AS conv_fb_onfb_lead_total_28c_1v,
        leads AS conv_fb_lead_total_28c_1v,
        purchase_total AS conv_fb_purchase_total_28c_1v,
        fb_pixel_purchase AS conv_fb_purchase_28c_1v,
        fb_mobile_purchase AS conv_fb_mobile_purchase_28c_1v,
        mobile_click_through AS conv_fb_mobile_purchase_click_through_28c,
        mobile_view_through AS conv_fb_mobile_purchase_view_through_1v,
        offsite_conversion_fb_pixel_purchase_1d_click AS conv_fb_purchase_1c,
        offsite_conversion_fb_pixel_purchase_7d_click AS conv_fb_purchase_7c,
        offsite_conversion_fb_pixel_purchase_28d_click AS conv_fb_purchase_28c,
        offsite_conversion_fb_pixel_purchase_1d_view AS conv_fb_purchase_1v,
        offsite_conversion_fb_pixel_purchase_value_1d_click AS conv_fb_value_purchase_1c,
        offsite_conversion_fb_pixel_purchase_value_7d_click AS conv_fb_value_purchase_7c,
        offsite_conversion_fb_pixel_purchase_value_28d_click AS conv_fb_value_purchase_28c,
        offsite_conversion_fb_pixel_purchase_value_1d_view AS conv_fb_value_purchase_1v,
        offsite_conversion_fb_pixel_purchase_value_28d_view AS conv_fb_value_purchase_28v,
        offsite_conversion_fb_pixel_purchase_1d_view_1d_click AS conv_fb_purchase_1c_1v,
        offsite_conversion_fb_pixel_purchase_1d_view_7d_click AS conv_fb_purchase_7c_1v,
        offsite_conversion_fb_pixel_purchase_value_1d_view_1d_click AS conv_fb_value_purchase_1c_1v,
        offsite_conversion_fb_pixel_purchase_value_1d_view_7d_click AS conv_fb_value_purchase_7c_1v,
        purchase_value AS conv_fb_value_purchase_28c_1v,
        revenue_click_through AS conv_fb_value_click_through_28c,
        revenue_view_through AS conv_fb_value_view_through_1v,
        search_total AS conv_fb_search_total_28c_1v,
        search AS conv_fb_search_28c_1v,
        start_trial_total AS conv_fb_start_trial_28c_1v,
        start_trial_total_value AS conv_fb_value_start_trial_28c_1v,
        start_trial_website AS conv_fb_website_start_trial_28c_1v,
        start_trial_mobile_app AS conv_fb_mobile_start_trial_28c_1v,
        submit_application_total AS conv_fb_submit_application_28c_1v,
        action_subscribe_total AS conv_fb_subscribe_total_28c_1v,
        subscribe_total AS conv_fb_subscribe_28c_1v,
        subscribe_total_value AS conv_fb_value_subscribe_28c_1v,
        view_content AS conv_fb_view_content_28c_1v,
        fb_mobile_content_view AS conv_fb_mobile_view_content_28c_1v,
        fb_offline_purchases AS conv_fb_offline_purchase_28c_1v,
        fb_offline_purchase_conv_value AS conv_fb_value_offline_purchase_28c_1v,
        omni_add_to_cart AS conv_fb_omni_add_to_cart_28c_1v,
        omni_complete_registration AS conv_fb_omni_complete_registration_28c_1v,
        omni_custom AS conv_fb_omni_custom_28c_1v,
        omni_initiated_checkout AS conv_fb_omni_initiated_checkout_28c_1v,
        omni_purchase AS conv_fb_omni_purchase_28c_1v,
        omni_search AS conv_fb_omni_search_28c_1v,
        omni_view_content AS conv_fb_omni_view_content_28c_1v,

        {#- Collaborative Ads Conversions -#}
        catalog_add_to_cart_total AS conv_fb_catalog_add_to_cart_28c_1v,
        catalog_add_to_cart_total_value AS conv_fb_value_catalog_add_to_cart_28c_1v,
        catalog_view_content_total AS conv_fb_catalog_view_content_28c_1v,
        catalog_view_content_total_value AS conv_fb_value_catalog_view_content_28c_1v,
        catalog_purchase_total AS conv_fb_catalog_purchase_28c_1v,
        catalog_purchase_total_value AS conv_fb_value_catalog_purchase_28c_1v,
        catalog_website_add_to_cart AS conv_fb_catalog_website_add_to_cart_28c_1v,
        catalog_website_add_to_cart_value AS conv_fb_value_catalog_website_add_to_cart_28c_1v,
        catalog_website_view_content AS conv_fb_catalog_website_view_content_28c_1v,
        catalog_website_view_content_value AS conv_fb_value_catalog_website_view_content_28c_1v,
        catalog_website_purchase AS conv_fb_catalog_website_purchase_28c_1v,
        catalog_website_purchase_value AS conv_fb_value_catalog_website_purchase_28c_1v,
        catalog_mobile_add_to_cart AS conv_fb_catalog_mobile_add_to_cart_28c_1v,
        catalog_mobile_add_to_cart_value AS conv_fb_value_catalog_mobile_add_to_cart_28c_1v,
        catalog_mobile_content_view AS conv_fb_catalog_mobile_content_view_28c_1v,
        catalog_mobile_content_view_value AS conv_fb_value_catalog_mobile_content_view_28c_1v,
        catalog_mobile_purchase AS conv_fb_catalog_mobile_purchase_28c_1v,
        catalog_mobile_purchase_value AS conv_fb_value_catalog_mobile_purchase_28c_1v,
        catalog_omni_add_to_cart AS conv_fb_catalog_omni_add_to_cart_28c_1v,
        catalog_omni_add_to_cart_value AS conv_fb_value_catalog_omni_add_to_cart_28c_1v,
        catalog_omni_view_content AS conv_fb_catalog_omni_view_content_28c_1v,
        catalog_omni_view_content_value AS conv_fb_value_catalog_omni_view_content_28c_1v,
        catalog_omni_purchase AS conv_fb_catalog_omni_purchase_28c_1v,
        catalog_omni_purchase_value AS conv_fb_value_catalog_omni_purchase_28c_1v

        {#- Custom conversions -#}

        {#- Loop through each custom conversion and rename column to include attribution model used -#}
        {%- for col in cols if "dynamic_" in col.column -%}

        ,
        {{ col.column }} AS conv_fb_custom_{{ col.column|replace("dynamic_", "") }}_28c_1v

        {%- endfor  %}

        -- Excluded fields --
        /*
        purchase, --This is duplicate field as fb_pixel_purchase
        frequency,
        cost_per_unique_landing_page_view,
        */

        -- Deprecated fields --
        /*
        unique_inline_link_clicks,
        unique_outbound_clicks,
        video_play,
        cost_per_10_sec_video_view,
        video_10_sec_watched_actions,
        video_avg_percent_watched_actions,
        lead_form,
        revenue,
        relevance_score,
        lead,
        conv_value,
        offsite_conversion,
        conv,
        unique_landing_page_view,
        */

    FROM source_data

    LEFT JOIN facebook_entity_ad_data
        ON source_data.ad_id = facebook_entity_ad_data.ad_id

    LEFT JOIN facebook_entity_adset_data
        ON source_data.adset_id = facebook_entity_adset_data.adset_id

    LEFT JOIN facebook_entity_campaign_data
        ON source_data.campaign_id = facebook_entity_campaign_data.campaign_id

    LEFT JOIN facebook_entity_creative_data
        ON source_data.creative_id = facebook_entity_creative_data.creative_id

    LEFT JOIN facebook_entity_account_data
        ON source_data.account_id = facebook_entity_account_data.account_id

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'ad_id', 'creative_id']) }} AS id,
        'Facebook Paid' AS data_source,

        CASE
            WHEN instagram_permalink_url IS NULL THEN 'Facebook'
            WHEN instagram_permalink_url IS NOT NULL THEN 'Instagram'
        END AS channel_source_name,

        'Paid' AS channel_source_type,
        'Paid Social' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final