WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'ga_session_performance') }}

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'source', 'medium', 'campaign_name', 'ad_content', 'keyword', 'user_type', 'device_category', 'landing_page_path']) }} AS id,
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
      
        SUM(sessions) AS website_sessions,
        SUM(session_duration) AS website_session_duration,
        SUM(bounces) AS bounces,
        SUM(page_views) AS page_views,
        SUM(transactions) AS transactions,
        SUM(transaction_revenue) AS transaction_revenue,
        SUM(adds_to_cart) AS adds_to_cart,
        SUM(goal_1_completions) AS goal_1_completions,
        SUM(goal_2_completions) AS goal_2_completions,
        SUM(goal_3_completions) AS goal_3_completions,
        SUM(goal_4_completions) AS goal_4_completions,
        SUM(goal_5_completions) AS goal_5_completions,
        SUM(goal_6_completions) AS goal_6_completions,
        SUM(goal_7_completions) AS goal_7_completions,
        SUM(goal_8_completions) AS goal_8_completions,
        SUM(goal_9_completions) AS goal_9_completions,
        SUM(goal_10_completions) AS goal_10_completions,
        SUM(goal_11_completions) AS goal_11_completions,
        SUM(goal_12_completions) AS goal_12_completions,
        SUM(goal_13_completions) AS goal_13_completions,
        SUM(goal_14_completions) AS goal_14_completions,
        SUM(goal_15_completions) AS goal_15_completions,
        SUM(goal_16_completions) AS goal_16_completions,
        SUM(goal_17_completions) AS goal_17_completions,
        SUM(goal_18_completions) AS goal_18_completions,
        SUM(goal_19_completions) AS goal_19_completions,
        SUM(goal_20_completions) AS goal_20_completions,
        SUM(total_goal_completions) AS total_goal_completions,
        SUM(goal_1_value) AS goal_1_value,
        SUM(goal_2_value) AS goal_2_value,
        SUM(goal_3_value) AS goal_3_value,
        SUM(goal_4_value) AS goal_4_value,
        SUM(goal_5_value) AS goal_5_value,
        SUM(goal_6_value) AS goal_6_value,
        SUM(goal_7_value) AS goal_7_value,
        SUM(goal_8_value) AS goal_8_value,
        SUM(goal_9_value) AS goal_9_value,
        SUM(goal_10_value) AS goal_10_value,
        SUM(goal_11_value) AS goal_11_value,
        SUM(goal_12_value) AS goal_12_value,
        SUM(goal_13_value) AS goal_13_value,
        SUM(goal_14_value) AS goal_14_value,
        SUM(goal_15_value) AS goal_15_value,
        SUM(goal_16_value) AS goal_16_value,
        SUM(goal_17_value) AS goal_17_value,
        SUM(goal_18_value) AS goal_18_value,
        SUM(goal_19_value) AS goal_19_value,
        SUM(goal_20_value) AS goal_20_value
    
    FROM source_data

    WHERE account_id IN {{ var('google_analytics_ids') }}

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12

)

SELECT * FROM final