{%- set source_account_ids = get_account_ids('google campaign manager') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ ref('base_google_campaign_manager__ads_placement_sites') }}

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
        source_data.campaign_start_date,
        source_data.campaign_end_date,
        source_data.site_id,
        source_data.site,
        source_data.site_directory,
        source_data.placement_id,
        source_data.placement,
        source_data.placement_compatibility,
        source_data.placement_cost_structure,
        source_data.placement_end_date,
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
                IFNULL(SAFE_CAST(source_data.video_length AS FLOAT64), 0)
                ) 
            AS STRING) AS video_length,
        source_data.currency,
        
        {#- General metrics -#}
        SUM(impressions) AS impressions,
        SUM(viewable_impressions) AS viewable_impressions,
        SUM(measurable_impressions) AS measurable_impressions,
        SUM(eligible_impressions) AS eligible_impressions,
        SUM(active_view_impressions) AS active_view_impressions,
        SUM(active_view_groupm_viewable_impressions) AS active_view_groupm_viewable_impressions,
        SUM(active_view_groupm_measurable_impressions) AS active_view_groupm_measurable_impressions,
        SUM(active_view_groupm_trv_viewable_impressions) AS active_view_groupm_trv_viewable_impressions,
        SUM(active_view_groupm_trv_measurable_impressions) AS active_view_groupm_trv_measurable_impressions,
        SUM(active_view_impression_distribution_not_measurable) AS active_view_impression_distribution_not_measurable,
        SUM(active_view_impression_distribution_not_viewable) AS active_view_impression_distribution_not_viewable,
        SUM(active_view_impression_distribution_viewable) AS active_view_impression_distribution_viewable,
        SUM(active_view_impressions_visible_ten_seconds) AS active_view_impressions_visible_ten_seconds,
        SUM(active_view_not_measurable_impressions) AS active_view_not_measurable_impressions,
        SUM(active_view_not_viewable_impressions) AS active_view_not_viewable_impressions,
        SUM(active_view_average_viewable_time_second) AS active_view_average_viewable_time_second,
        SUM(link_clicks) AS link_clicks,
        SUM(cost) AS cost,
        SUM(planned_media_cost) AS planned_media_cost,
        SUM(dbm_cost) AS dbm_cost,
        SUM(dbm_cost_usd) AS dbm_cost_usd,

        {#- Video metrics -#}
        SUM(video_views) AS video_views,
        SUM(video_p25_watched) AS video_p25_watched,
        SUM(video_p50_watched) AS video_p50_watched,
        SUM(video_p75_watched) AS video_p75_watched,
        SUM(video_completions) AS video_completions,
        SUM(video_progress_events) AS video_progress_events,
        SUM(video_full_screens) AS video_full_screens,
        SUM(video_full_screen_plays) AS video_full_screen_plays,
        SUM(video_full_screen_completes) AS video_full_screen_completes,
        SUM(video_average_view_time) AS video_average_view_time,
        SUM(video_companion_impressions) AS video_companion_impressions,
        SUM(video_companion_clicks) AS video_companion_clicks,
        SUM(video_interactions) AS video_interactions,
        SUM(video_mutes) AS video_mutes,
        SUM(video_unmutes) AS video_unmutes,
        SUM(video_pauses) AS video_pauses,
        SUM(video_skips) AS video_skips,
        SUM(video_stops) AS video_stops,
        SUM(video_replays) AS video_replays,
        
        {#- Rich media metrics -#}
        SUM(rich_media_average_display_time) AS rich_media_average_display_time,
        SUM(rich_media_average_expansion_time) AS rich_media_average_expansion_time,
        SUM(rich_media_average_interaction_time) AS rich_media_average_interaction_time,
        SUM(rich_media_custom_average_time) AS rich_media_custom_average_time,
        SUM(rich_media_backup_images) AS rich_media_backup_images,
        SUM(rich_media_event_counters) AS rich_media_event_counters,
        SUM(rich_media_event_timers) AS rich_media_event_timers,
        SUM(rich_media_custom_exits) AS rich_media_custom_exits,
        SUM(rich_media_expansion_time) AS rich_media_expansion_time,
        SUM(rich_media_expansions) AS rich_media_expansions,
        SUM(rich_media_average_full_screen_view_time) AS rich_media_average_full_screen_view_time,
        SUM(rich_media_full_screen_impressions) AS rich_media_full_screen_impressions,
        SUM(rich_media_interactive_impressions) AS rich_media_interactive_impressions,
        SUM(rich_media_manual_closes) AS rich_media_manual_closes,
        SUM(rich_media_clicks) AS rich_media_clicks,
        SUM(rich_media_impressions) AS rich_media_impressions,
        SUM(rich_media_custom_timers) AS rich_media_custom_timers,
        SUM(rich_media_display_time) AS rich_media_display_time,
        SUM(rich_media_interaction_time) AS rich_media_interaction_time,
        SUM(rich_media_interactions) AS rich_media_interactions,
        SUM(rich_media_true_view_views) AS rich_media_true_view_views

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

    {{ dbt_utils.group_by(n=36) }}

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