WITH

data AS (

    SELECT * FROM {{ ref('stg_pinterest_ads__pins_1v_30en_30cl') }}

),

general_definitions AS (

    SELECT
    
        *,
        'Pinterest Paid' AS data_source,
        'Pinterest' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Social' AS channel_name
  
    FROM data
    
),

final AS (

    SELECT
    
        id,
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        date,
        internal_account_id,
        internal_account_name,
        campaign_id,
        campaign_name,
        campaign_status,
        campaign_managed_status,
        ad_group_id,
        ad_group_name,
        ad_group_status,
        pin_id,
        pin_promotion_id,
        pin_promotion_name,
        pin_link,
        pin_promotion_status,
        
        paid_impressions,
        earned_impressions,
        paid_clicks AS link_clicks,
        spend AS cost,
        paid_closeups,
        earned_closeups,
        paid_engagements,
        downstream_engagements,
        paid_saves,
        video_avg_watchtime_in_second_earned,
        video_avg_watchtime_in_second_paid,
        video_mrc_views_earned,
        video_mrc_views_paid,
        video_starts,
        video_starts_earned,
        video_p100_complete_1,
        video_p100_complete_2,
        video_p25_combined_1,
        video_p25_combined_2,
        video_p50_combined_1,
        video_p50_combined_2,
        video_p75_combined_1,
        video_p75_combined_2,
        video_p95_combined_1,
        video_p95_combined_2
        
     FROM general_definitions

)

SELECT * FROM final