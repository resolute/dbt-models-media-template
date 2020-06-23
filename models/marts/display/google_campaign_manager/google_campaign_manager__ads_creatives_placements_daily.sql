WITH

data AS (
  
    SELECT * FROM {{ ref('stg_google_campaign_manager__ads_placement_sites') }}

),
  
extract_utm_params AS (

    SELECT
    
        *,
        REGEXP_EXTRACT(click_through_url,r'[?&]utm_source=([^&]+)') as utm_source,
        REGEXP_EXTRACT(click_through_url,r'[?&]utm_medium=([^&]+)') as utm_medium,
        REGEXP_EXTRACT(click_through_url,r'[?&]utm_campaign=([^&]+)') as utm_campaign,
        REGEXP_EXTRACT(click_through_url,r'[?&]utm_content=([^&]+)') as utm_content,
        REGEXP_EXTRACT(click_through_url,r'[?&]utm_term=([^&]+)') as utm_term
    
    FROM data
    
),
  
general_definitions AS (

    SELECT
    
        *,
        'Campaign Manager' AS data_source,
        'Campaign Manager' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Display' AS channel_name
  
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
        advertiser_id,
        advertiser_name,
        campaign_id,
        campaign_name,
        campaign_start_date,
        campaign_end_date,
        site_id_dcm,
        site_dcm,
        site_sirectory,
        placement_id,
        placement,
        placement_compatibility,
        placement_cost_structure,
        placement_end_date,
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
        video_length
        
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
        click_rate,
        mediacost AS cost,
        planned_media_cost,
        dbm_cost,
        dbm_cost_usd,
        effective_cpm,
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
        video_companion_clicks,
        video_companion_impressions,
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
        rich_media_full_screen_video_completes,
        rich_media_full_screen_video_plays,
        rich_media_interaction_rate,
        rich_media_interactive_impressions,
        rich_media_manual_closes,
        rich_media_clicks,
        rich_media_impressions,
        rich_media_custom_timers,
        rich_media_display_time_dimension,
        rich_media_interaction_time_dimension,
        rich_media_interactions,
        rich_media_true_view_views,
        rich_media_average_video_view_time,
        rich_media_video_companion_clicks,
        rich_media_video_completions,
        rich_media_video_full_screens,
        rich_media_video_interaction_rate,
        rich_media_video_interactions,
        rich_media_video_mutes,
        rich_media_video_pauses,
        rich_media_video_plays,
        rich_media_video_progress_events,
        rich_media_video_replays,
        rich_media_video_skips,
        rich_media_video_stops,
        rich_media_video_unmutes,
        rich_media_video_view_rate,
        rich_media_video_views,
        video_first_quartile_completions,
        video_midpoints,
        video_third_quartile_completions
        
     FROM general_definitions

),

final AS (

    SELECT
        
        *,
        DATE_DIFF(date, campaign_start_date, DAY) AS ad_age_in_days
    
    FROM rename_columns_and_set_defaults

)
  
SELECT * FROM final