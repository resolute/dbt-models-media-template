WITH

source_data AS (

    SELECT * FROM {{ source('google_analytics', 'view_ga_session_performance') }}

),

final AS (
  
    SELECT 
    
        account_id,
        account_name,
        date,
        source AS utm_source,
        medium AS utm_medium,
        campaign_name AS utm_campaign,
        ad_content AS utm_content,
        keyword AS utm_term,
        user_type,
        device_category,
        landing_page_path,
      
        sessions AS website_sessions,
        session_duration AS website_session_duration,
        bounces,
        page_views,
        transactions,
        transaction_revenue,
        adds_to_cart,
        goal_1_completions,
        goal_2_completions,
        goal_3_completions,
        goal_4_completions,
        goal_5_completions,
        goal_6_completions,
        goal_7_completions,
        goal_8_completions,
        goal_9_completions,
        goal_10_completions,
        goal_11_completions,
        goal_12_completions,
        goal_13_completions,
        goal_14_completions,
        goal_15_completions,
        goal_16_completions,
        goal_17_completions,
        goal_18_completions,
        goal_19_completions,
        goal_20_completions,
        total_goal_completions,
        goal_1_value,
        goal_2_value,
        goal_3_value,
        goal_4_value,
        goal_5_value,
        goal_6_value,
        goal_7_value,
        goal_8_value,
        goal_9_value,
        goal_10_value,
        goal_11_value,
        goal_12_value,
        goal_13_value,
        goal_14_value,
        goal_15_value,
        goal_16_value,
        goal_17_value,
        goal_18_value,
        goal_19_value,
        goal_20_value
    
    FROM source_data

    WHERE account_id = '50958144'

)

SELECT * FROM final