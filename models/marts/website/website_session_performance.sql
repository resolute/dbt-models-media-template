WITH

website_session_performance_news_site AS (
  
    SELECT * FROM {{ ref('google_analytics__session_performance_50958144') }}

),

website_session_performance_main_site AS (
  
    SELECT * FROM {{ ref('google_analytics__session_performance_109800238') }}

),

website_session_performance_alumni_site AS (
  
    SELECT * FROM {{ ref('google_analytics__session_performance_183763846') }}

),

website_article_performance AS (
  
    SELECT * FROM {{ ref('website_article_performance') }}

),

website_alumni_event_page_performance AS (
  
    SELECT * FROM {{ ref('google_analytics__alumni_events_page_performance') }}

),

website_article_performance_rollup AS (
  
    SELECT * FROM website_session_performance_news_site

    UNION ALL
    
    SELECT * FROM website_session_performance_main_site
    
    UNION ALL
    
    SELECT * FROM website_session_performance_alumni_site
    
),

reduce_grain_website_session_performance AS (

    SELECT
        
        data_source,
        account_id,
        account_name,
        date,
        channel_name,
        channel_source_name,
        channel_source_type,
        
        SUM(website_sessions) AS website_sessions,
        SUM(website_session_duration) AS website_session_duration,
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
    
    FROM website_article_performance_rollup
    
    GROUP BY 1,2,3,4,5,6,7
    
),

reduce_grain_website_article_performance AS (

    SELECT
    
        data_source,
        account_id,
        account_name,
        date,
        channel_name,
        channel_source_name,
        channel_source_type,
        
        SUM(website_article_views) AS website_article_views,
        SUM(website_article_read) AS website_article_read,
        SUM(website_article_shares) AS website_article_shares,
        SUM(website_article_engaged_time_total) AS website_article_engaged_time_total
    
    FROM website_article_performance
    
    GROUP BY 1,2,3,4,5,6,7
    
),

reduce_grain_website_alumni_event_page_performance AS (

    SELECT
    
        data_source,
        account_id,
        account_name,
        date,
        channel_name,
        channel_source_name,
        channel_source_type,
        
        SUM(website_alumni_events_pageviews) AS website_alumni_events_pageviews,
        SUM(website_alumni_events_pageviews_unique) AS website_alumni_events_pageviews_unique
    
    FROM website_alumni_event_page_performance
    
    GROUP BY 1,2,3,4,5,6,7
    
),

join_data AS (
    
    SELECT
    
        COALESCE(sessions.data_source, articles.data_source) AS data_source,
        COALESCE(sessions.account_id, articles.account_id) AS account_id,
        COALESCE(sessions.account_name, articles.account_name) AS account_name,
        COALESCE(sessions.date, articles.date) AS date,
        COALESCE(sessions.channel_name, articles.channel_name) AS channel_name,
        COALESCE(sessions.channel_source_name, articles.channel_source_name) AS channel_source_name,
        COALESCE(sessions.channel_source_type, articles.channel_source_type) AS channel_source_type,
        
        COALESCE(website_sessions,0) AS website_sessions,
        COALESCE(website_session_duration,0) AS website_session_duration,
        COALESCE(bounces,0) AS bounces,
        COALESCE(page_views,0) AS page_views,
        COALESCE(transactions,0) AS transactions,
        COALESCE(transaction_revenue,0) AS transaction_revenue,
        COALESCE(adds_to_cart,0) AS adds_to_cart,
        COALESCE(goal_1_completions,0) AS goal_1_completions,
        COALESCE(goal_2_completions,0) AS goal_2_completions,
        COALESCE(goal_3_completions,0) AS goal_3_completions,
        COALESCE(goal_4_completions,0) AS goal_4_completions,
        COALESCE(goal_5_completions,0) AS goal_5_completions,
        COALESCE(goal_6_completions,0) AS goal_6_completions,
        COALESCE(goal_7_completions,0) AS goal_7_completions,
        COALESCE(goal_8_completions,0) AS goal_8_completions,
        COALESCE(goal_9_completions,0) AS goal_9_completions,
        COALESCE(goal_10_completions,0) AS goal_10_completions,
        COALESCE(goal_11_completions,0) AS goal_11_completions,
        COALESCE(goal_12_completions,0) AS goal_12_completions,
        COALESCE(goal_13_completions,0) AS goal_13_completions,
        COALESCE(goal_14_completions,0) AS goal_14_completions,
        COALESCE(goal_15_completions,0) AS goal_15_completions,
        COALESCE(goal_16_completions,0) AS goal_16_completions,
        COALESCE(goal_17_completions,0) AS goal_17_completions,
        COALESCE(goal_18_completions,0) AS goal_18_completions,
        COALESCE(goal_19_completions,0) AS goal_19_completions,
        COALESCE(goal_20_completions,0) AS goal_20_completions,
        COALESCE(total_goal_completions,0) AS total_goal_completions,
        COALESCE(goal_1_value,0) AS goal_1_value,
        COALESCE(goal_2_value,0) AS goal_2_value,
        COALESCE(goal_3_value,0) AS goal_3_value,
        COALESCE(goal_4_value,0) AS goal_4_value,
        COALESCE(goal_5_value,0) AS goal_5_value,
        COALESCE(goal_6_value,0) AS goal_6_value,
        COALESCE(goal_7_value,0) AS goal_7_value,
        COALESCE(goal_8_value,0) AS goal_8_value,
        COALESCE(goal_9_value,0) AS goal_9_value,
        COALESCE(goal_10_value,0) AS goal_10_value,
        COALESCE(goal_11_value,0) AS goal_11_value,
        COALESCE(goal_12_value,0) AS goal_12_value,
        COALESCE(goal_13_value,0) AS goal_13_value,
        COALESCE(goal_14_value,0) AS goal_14_value,
        COALESCE(goal_15_value,0) AS goal_15_value,
        COALESCE(goal_16_value,0) AS goal_16_value,
        COALESCE(goal_17_value,0) AS goal_17_value,
        COALESCE(goal_18_value,0) AS goal_18_value,
        COALESCE(goal_19_value,0) AS goal_19_value,
        COALESCE(goal_20_value,0) AS goal_20_value,
        COALESCE(website_article_views,0) AS website_article_views,
        COALESCE(website_article_read,0) AS website_article_read,
        COALESCE(website_article_shares,0) AS website_article_shares,
        COALESCE(website_article_engaged_time_total,0) AS website_article_engaged_time_total,
        COALESCE(website_alumni_events_pageviews,0) AS website_alumni_events_pageviews,
        COALESCE(website_alumni_events_pageviews_unique,0) AS website_alumni_events_pageviews_unique
        
    FROM reduce_grain_website_session_performance AS sessions
    
    FULL JOIN reduce_grain_website_article_performance AS articles USING(data_source, account_id, date, channel_name, channel_source_name, channel_source_type)
    
    FULL JOIN reduce_grain_website_alumni_event_page_performance AS alumni USING(data_source, account_id, date, channel_name, channel_source_name, channel_source_type)

),

final AS (

    SELECT

        {{ dbt_utils.surrogate_key(['date', 'account_id', 'channel_name', 'channel_source_name', 'channel_source_type']) }} AS id,
        *

    FROM join_data
)
      
SELECT * FROM final