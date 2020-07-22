{%- set source_account_ids = var('google_campaign_manager_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'dcm_ads_placement_sites') }}

    WHERE advertiser_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        account_id,
        account_name,
        date,
        advertiser_id,
        advertiser_name,
        campaign_id,
        campaign_name,
        CAST(campaign_start_date AS DATE) AS campaign_start_date,
        CAST(campaign_end_date AS DATE) AS campaign_end_date,
        site_id_dcm AS site_id,
        site_dcm AS site,
        site_sirectory AS site_directory,
        placement_id,
        placement,
        placement_compatibility,
        placement_cost_structure,
        CAST(placement_end_date AS DATE) AS placement_end_date,
        placement_size,
        placement_strategy,
        platform_type,
        ad_id,
        ad,
        ad_status,
        ad_type,
        creative_group_1,
        creative_group_2,
        creative_id,
        creative,     
        asset,
        asset_category,
        browser_platform,
        click_through_url,
        connection_type,
        content_category,
        video_length,
        
        {#- General metrics -#}
        impressions,
        viewable_impressions,
        measurable_impressions,
        eligible_impressions,
        active_view_impressions,
        active_view_groupm_viewable_impressions,
        active_view_groupm_measurable_impressions,
        active_view_groupm_TrvViewableImpressions,
        active_view_groupm_TrvMeasurableImpressions,
        active_view_impression_distribution_not_measurable,
        active_view_impression_distribution_not_viewable,
        active_view_impression_distribution_viewable,
        active_view_impressions_visible_ten_seconds,
        active_view_not_measurable_impressions,
        active_view_not_viewable_impressions,
        active_view_Average_ViewableTimeSecond,
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
        rich_media_display_time_dimension,
        rich_media_interaction_time_dimension,
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

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'advertiser_id', 'campaign_id', 'site_id', 'placement_id', 'ad_id', 'creative_id']) }} AS id,
        'Campaign Manager' AS data_source,
        'Campaign Manager' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Display' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final