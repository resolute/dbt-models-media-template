WITH

--Email Data
email AS (
    
    SELECT * FROM {{ ref('email_performance') }}

),

--Social Organic Data
social_organic AS (
    
    SELECT * FROM {{ ref('social_organic_posts_rollup') }}

),
    
--Social Paid Data
social_paid AS (
    
    SELECT * FROM {{ ref('social_paid_ads_rollup') }}

),

--Social Followers Data
social_followers AS (
    
    SELECT * FROM {{ ref('social_organic_followers_rollup') }}

),

--Website Session Performance Data
website_session_performance AS (
    
    SELECT * FROM {{ ref('website_session_performance') }}

),

reduce_columns_email AS (
    
    SELECT
        
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        email_original_send_date AS date,
        
        0 AS social_impressions,
        0 AS social_engagements,
        0 AS social_link_clicks,
        SUM(sends) AS email_sends,
        SUM(opens_unique) AS email_opens,
        SUM(clicks_unique) AS email_clicks,
        0 AS website_sessions,
        0 AS website_article_views,
        0 AS website_article_reads,
        0 AS website_alumni_search_events,
        0 AS social_followers_total,
        0 AS social_followers_net,
        0 AS email_subscribers
        
    FROM email
    
    GROUP BY 1,2,3,4,5,6,7

),

reduce_columns_social_organic AS (
    
    SELECT
        
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        post_publish_date AS date,
        
        SUM(impressions) AS social_impressions,
        SUM(engagements) AS social_engagements,
        SUM(link_clicks) AS social_link_clicks,
        0 AS email_sends,
        0 AS email_opens,
        0 AS email_clicks,
        0 AS website_sessions,
        0 AS website_article_views,
        0 AS website_article_reads,
        0 AS website_alumni_search_events,
        0 AS social_followers_total,
        0 AS social_followers_net,
        0 AS email_subscribers
        
    FROM social_organic
    
    GROUP BY 1,2,3,4,5,6,7

),

reduce_columns_social_paid AS (
    
    SELECT
        
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        date,
        
        SUM(impressions) AS social_impressions,
        SUM(engagements) AS social_engagements,
        SUM(link_clicks) AS social_link_clicks,
        0 AS email_sends,
        0 AS email_opens,
        0 AS email_clicks,
        0 AS website_sessions,
        0 AS website_article_views,
        0 AS website_article_reads,
        0 AS website_alumni_search_events,
        0 AS social_followers_total,
        0 AS social_followers_net,
        0 AS email_subscribers
        
    FROM social_paid
    
    GROUP BY 1,2,3,4,5,6,7

),

reduce_columns_social_followers AS (
    
    SELECT
        
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        date,
        
        0 AS social_impressions,
        0 AS social_engagements,
        0 AS social_link_clicks,
        0 AS email_sends,
        0 AS email_opens,
        0 AS email_clicks,
        0 AS website_sessions,
        0 AS website_article_views,
        0 AS website_article_reads,
        0 AS website_alumni_search_events,
        SUM(social_followers_total) AS social_followers_total,
        SUM(social_followers_net) AS social_followers_net,
        0 AS email_subscribers
        
    FROM social_followers
    
    GROUP BY 1,2,3,4,5,6,7

),

reduce_columns_website_session_performance AS (
    
    SELECT
        
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        date,
        
        0 AS social_impressions,
        0 AS social_engagements,
        0 AS social_link_clicks,
        0 AS email_sends,
        0 AS email_opens,
        0 AS email_clicks,
        SUM(website_sessions) AS website_sessions,
        SUM(website_article_views) AS website_article_views,
        SUM(website_article_read) AS website_article_reads,
        SUM(website_alumni_events_pageviews) AS website_alumni_search_events,
        0 AS social_followers_total,
        0 AS social_followers_net,
        0 AS email_subscribers
        
    FROM website_session_performance
    
    GROUP BY 1,2,3,4,5,6,7

),

--Union all data
  
union_all_data AS (
  
    SELECT * FROM reduce_columns_email

    UNION ALL
    
    SELECT * FROM reduce_columns_social_organic

    UNION ALL
    
    SELECT * FROM reduce_columns_social_paid
    
    UNION ALL
    
    SELECT * FROM reduce_columns_social_followers
    
    UNION ALL
    
    SELECT * FROM reduce_columns_website_session_performance
    
),

final AS (
  
    SELECT
        
        {{ dbt_utils.surrogate_key(['data_source', 'account_id','channel_source_name', 'channel_source_type', 'channel_name', 'date']) }} AS id,
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        date,
        
        SUM(social_impressions) AS social_impressions,
        SUM(social_engagements) AS social_engagements,
        SUM(social_link_clicks) AS social_link_clicks,
        SUM(email_sends) AS email_sends,
        SUM(email_opens) AS email_opens,
        SUM(email_clicks) AS email_clicks,
        SUM(website_sessions) AS website_sessions,
        SUM(website_article_views) AS website_article_views,
        SUM(website_article_reads) AS website_article_reads,
        SUM(website_alumni_search_events) AS website_alumni_search_events,
        SUM(social_followers_total) AS social_followers_total,
        SUM(social_followers_net) AS social_followers_net,
        SUM(email_subscribers) AS email_subscribers
        
    FROM union_all_data
    
    GROUP BY 1,2,3,4,5,6,7,8
    
)

SELECT * FROM final