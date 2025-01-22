{%- set source_account_ids = get_account_ids('google campaign manager') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_cm_ads_placement_sites') }}

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
        CAST(source_data.campaign_start_date AS DATE) AS campaign_start_date,
        CAST(source_data.campaign_end_date AS DATE) AS campaign_end_date,
        source_data.site_id_dcm AS site_id,
        source_data.site_dcm AS site,
        source_data.site_sirectory AS site_directory,
        source_data.placement_id,
        source_data.placement,
        source_data.placement_compatibility,
        source_data.placement_cost_structure,
        CAST(source_data.placement_end_date AS DATE) AS placement_end_date,
        source_data.placement_size,
        source_data.placement_strategy,
        source_data.platform_type,
        source_data.ad_id,
        source_data.ad,
        source_data.ad_status,
        source_data.ad_type,
        source_data.creative_group_1,
        source_data.creative_group_2,
        source_data.creative_id,
        COALESCE(creative.creative, source_data.creative) AS creative,     
        source_data.asset,
        source_data.asset_category,
        source_data.browser_platform,
        source_data.click_through_url,
        source_data.connection_type,
        source_data.content_category,
        -- Remove any decimals and set null values to zero from video length
        CAST(
            ROUND(
                IFNULL(SAFE_CAST(video_length AS FLOAT64), 0)
                ) 
            AS STRING) AS video_length,
        currency,
        
        {#- General metrics -#}
        impressions,
        viewable_impressions,
        measurable_impressions,
        eligible_impressions,
        active_view_impressions,
        active_view_groupm_viewable_impressions,
        active_view_groupm_measurable_impressions,
        active_view_groupm_trv_viewable_impressions,
        active_view_groupm_trv_measurable_impressions,
        active_view_impression_distribution_not_measurable,
        active_view_impression_distribution_not_viewable,
        active_view_impression_distribution_viewable,
        active_view_impressions_visible_ten_seconds,
        active_view_not_measurable_impressions,
        active_view_not_viewable_impressions,
        active_view_average_viewable_time_second,
        clicks AS link_clicks,
        mediacost AS cost,
        planned_media_cost,
        dbm_cost,
        dbm_cost_usd,

        {#- Video metrics -#}
        rich_media_video_plays AS video_views,
        video_first_quartile_completions AS video_p25_watched,
        video_midpoints AS video_p50_watched,
        video_third_quartile_completions AS video_p75_watched,
        rich_media_video_completions AS video_completions,
        rich_media_video_progress_events AS video_progress_events,
        rich_media_video_full_screens AS video_full_screens,
        rich_media_full_screen_video_plays AS video_full_screen_plays,
        rich_media_full_screen_video_completes AS video_full_screen_completes,
        rich_media_average_video_view_time AS video_average_view_time,
        video_companion_impressions,
        video_companion_clicks,
        rich_media_video_interactions AS video_interactions,
        rich_media_video_mutes AS video_mutes,
        rich_media_video_unmutes AS video_unmutes,
        rich_media_video_pauses AS video_pauses,
        rich_media_video_skips AS video_skips,
        rich_media_video_stops AS video_stops,
        rich_media_video_replays AS video_replays,
        
        {#- Rich media metrics -#}
        rich_media_average_display_time,
        rich_media_average_expansion_time,
        rich_media_average_interaction_time,
        rich_media_custom_average_time,
        rich_media_backup_images,
        rich_media_event_counters,
        rich_media_event_timers,
        rich_media_custom_exits,
        rich_media_expansion_time,
        rich_media_expansions,
        rich_media_average_full_screen_view_time,
        rich_media_full_screen_impressions,
        rich_media_interactive_impressions,
        rich_media_manual_closes,
        rich_media_clicks,
        rich_media_impressions,
        rich_media_custom_timers,
        rich_media_display_time,
        rich_media_interaction_time,
        rich_media_interactions,
        rich_media_true_view_views

        -- Excluded fields --
        /*
        total_conversions,
        view_through_conversions,
        click_through_conversions,
        total_conversions_revenue,
        view_through_revenue,
        click_through_revenue,
        revenue_per_click,
        revenue_per_thousand_impressions,
        activity_delivery_status,
        activity_per_click,
        activity_per_thousand_impressions,
        click_delivery_status,
        click_rate,
        effective_cpm,
        rich_media_video_view_rate,
        rich_media_video_views,
        rich_media_video_interaction_rate,
        rich_media_video_companion_clicks, --duplicate to video_companion_clicks
        rich_media_interaction_rate,
        */

    FROM source_data

    LEFT JOIN gcm_entity_creative_data AS creative
        ON source_data.creative_id = creative.creative_id

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'advertiser_id', 'site_id', 'placement_id', 'placement_size', 'ad_id', 'creative_id', 'asset', 'platform_type', 'browser_platform', 'connection_type']) }} AS id,
        'Campaign Manager' AS data_source,
        'Campaign Manager' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Display' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final