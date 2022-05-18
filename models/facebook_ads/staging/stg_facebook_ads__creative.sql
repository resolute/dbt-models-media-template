{%- set source_account_ids = get_account_ids('facebook ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

{# Get a list of the columns from the upstream model #}
{%- set cols = adapter.get_columns_in_relation(source('improvado', 'facebook_ads_creative_platform')) -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'facebook_ads_creative_platform') }}

    WHERE REPLACE(account_id, 'act_', '') IN (SELECT REPLACE(x, 'act_', '') FROM UNNEST({{ source_account_ids }}) AS x)

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
        account.account_name AS account_name,
        source_data.date,
        source_data.campaign_id,
        campaign.campaign_name AS campaign_name,
        campaign.start_time AS campaign_start_time,
        campaign.stop_time AS campaign_stop_time,
        campaign.objective AS campaign_objective,
        campaign.buying_type AS campaign_buying_type,
        campaign.status AS campaign_status,
        campaign.effective_status AS campaign_effective_status,
        campaign.configured_status AS campaign_configured_status,
        source_data.adset_id,
        adset.adset_name AS adset_name,
        adset.destination_type AS adset_destination_type,
        adset.effective_status AS adset_effective_status,
        adset.optimization_goal AS adset_optimization_goal,
        adset.targeting AS adset_targeting,        
        source_data.ad_id,
        ad.ad_name AS ad_name,
        ad.status AS ad_status,
        ad.effective_status AS ad_effective_status,
        ad.created_time AS ad_created_time,
        source_data.creative_id,
        creative.creative_name AS creative_name,
        creative.status AS creative_status,
        creative.title AS creative_title,
        creative.description AS creative_description,
        creative.body AS creative_body,
        creative.caption AS creative_caption,
        creative.creative_link,
        creative.link_og_id AS creative_link_og_id,
        creative.link_url AS creative_link_url,
        creative.thumbnail_url AS creative_thumbnail_url,
        creative.image_url AS creative_image_url,
        creative.url_tags AS creative_url_tags,
        creative.object_type AS creative_object_type,
        creative.call_to_action_type AS creative_call_to_action_type,
        creative.website_destination_url AS creative_destination_url,
        creative.template_url AS creative_template_url,
        creative.object_id AS creative_object_id,
        creative.object_story_id AS creative_object_story_id,
        creative.effective_object_story_id AS creative_effective_object_story_id,
        creative.object_url AS creative_object_url,
        creative.actor_id AS creative_actor_id,
        creative.video_id AS creative_video_id,
        creative.instagram_actor_id AS creative_instagram_actor_id,
        creative.instagram_story_id AS creative_instagram_story_id,
        creative.effective_instagram_story_id AS creative_effective_instagram_story_id,
        creative.source_instagram_media_id AS creative_source_instagram_media_id,
        creative.instagram_permalink_url AS creative_instagram_permalink_url,
        creative.branded_content_sponsor_page_id AS creative_branded_content_sponsor_page_id,
        creative.lead_gen_form_id AS creative_lead_gen_form_id,
        source_data.publisher_platform,

        {#- General metrics -#}
        reach,
        impressions,
        spend AS cost,
        clicks,
        link_click AS link_clicks,
        outbound_clicks,

        {#- Engagement metrics -#}
        post_engagement AS post_engagements,
        post_reaction AS post_reactions,
        action_comment_value AS post_comments,
        action_post_value AS post_shares,
        action_like_value AS post_likes,

        {#- Video metrics -#}
        views AS video_views,
        video_play_actions AS video_plays,
        video_p25_watched_actions AS video_p25_watched,
        video_p50_watched_actions AS video_p50_watched,
        video_p75_watched_actions AS video_p75_watched,
        video_p100_watched_actions AS video_completions,
        thru_play AS video_thru_play,

        {#- Conversions -#}
        purchase AS conv_fb_purchase_28c_1v,
        purchase_value AS conv_fb_value_purchase_28c_1v,
        fb_mobile_purchase AS conv_fb_mobile_purchase_28c_1v,
        fb_mobile_purchase_value AS conv_fb_value_mobile_purchase_28c_1v,
        add_to_cart AS conv_fb_add_to_cart_28c_1v,
        add_to_cart_value AS conv_fb_value_add_to_cart_28c_1v,
        complete_registration AS conv_fb_complete_registration_28c_1v,
        complete_registration_value AS conv_fb_value_complete_registration_28c_1v,
        leads AS conv_fb_lead_total_28c_1v,
        lead_value AS conv_fb_value_lead_total_28c_1v,
        mobile_app_install AS conv_fb_mobile_app_install_28c_1v,
        mobile_app_instal_value AS conv_fb_value_mobile_app_install_28c_1v,
        view_content AS conv_fb_view_content

        {#- Custom conversions -#}

        {#- Loop through each custom conversion and rename column to include attribution model used -#}
        {%- for col in cols if "dynamic_" in col.column -%}

        ,
        {{ col.column }} AS conv_fb_custom_{{ col.column|replace("dynamic_", "") }}_28c_1v

        {%- endfor  %}

        -- Excluded fields --
        /*
        */

    FROM source_data

    LEFT JOIN facebook_entity_ad_data AS ad
        ON source_data.ad_id = ad.ad_id

    LEFT JOIN facebook_entity_adset_data AS adset
        ON source_data.adset_id = adset.adset_id

    LEFT JOIN facebook_entity_campaign_data AS campaign
        ON source_data.campaign_id = campaign.campaign_id

    LEFT JOIN facebook_entity_creative_data AS creative
        ON source_data.creative_id = creative.creative_id

    LEFT JOIN facebook_entity_account_data AS account
        ON source_data.account_id = account.account_id

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'ad_id', 'creative_id', 'publisher_platform']) }} AS id,
        'Facebook Paid' AS data_source,

        CASE
            WHEN publisher_platform = 'facebook' THEN 'Facebook'
            WHEN publisher_platform = 'instagram' THEN 'Instagram'
            ELSE publisher_platform
        END AS channel_source_name,

        'Paid' AS channel_source_type,
        'Paid Social' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final