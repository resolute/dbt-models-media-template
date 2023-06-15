{%- set source_account_ids = get_account_ids('pinterest ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'pinterest_ads_pins_1v_30en_30cl') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT
        
        {# Dimensions -#}
        account_id,
        account_name,
        date,
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
        
        {#- General metrics -#}
        paid_impressions,
        earned_impressions,
        spend AS cost,
        spend_in_microdollar AS cost_in_microdollar,
        paid_clicks,
        clickthrough_2 AS earned_clicks,

        {#- Engagement metrics -#}
        paid_closeups,
        earned_closeups,
        paid_engagements,
        downstream_engagements AS earned_engagements,
        paid_saves,
        repin_2 AS earned_saves,

        {#- Video metrics -#}
        video_starts AS video_starts_paid,
        video_starts_earned,
        video_p25_combined_1 AS video_p25_watched_paid,
        video_p25_combined_2 AS video_p25_watched_earned,
        video_p50_combined_1 AS video_p50_watched_paid,
        video_p50_combined_2 AS video_p50_watched_earned,
        video_p75_combined_1 AS video_p75_watched_paid,
        video_p75_combined_2 AS video_p75_watched_earned,
        video_p95_combined_1 AS video_p95_watched_paid,
        video_p95_combined_2 AS video_p95_watched_earned,
        video_p100_complete_1 AS video_completions_paid,
        video_p100_complete_2 AS video_completions_earned,
        video_mrc_views_paid,
        video_mrc_views_earned,   
        video_avg_watchtime_in_second_earned,
        video_avg_watchtime_in_second_paid,

        {#- Conversions -#}
        conversions AS conv_pi_total_conversions_30c_1v,
        revenue AS conv_pi_value_total_conversions_30c_1v,
        total_conversions_value_in_micro_dollar AS conv_pi_value_total_conversions_in_micro_dollar_30c_1v

        -- Excluded fields --
        /*
        internal_account_id, --This is duplicate field as account_id
        internal_account_name, --This is duplicate field as account_name
        */

        -- Deprecated fields --
        /*
        */

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'pin_promotion_id']) }} AS id,
        'Pinterest Paid' AS data_source,
        'Pinterest' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Social' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final