WITH

ga_article_performance AS (
  
    SELECT * FROM {{ ref('google_analytics__article_performance') }}

),

ga_article_dimensions AS (
  
    SELECT * FROM {{ ref('google_analytics__article_dimensions') }}

),
  
join_ga_article_performance_and_dimensions AS (
    
    SELECT
    
        ga_article_performance.data_source AS data_source,
        ga_article_performance.account_id AS account_id,
        ga_article_performance.account_name AS account_name,
        ga_article_performance.date AS date,
        ga_article_performance.website_article_id AS website_article_id,
        COALESCE(ga_article_performance.website_page_path, ga_article_performance.website_page_path) AS website_page_path,
        ga_article_dimensions.website_article_title AS website_article_title,
        ga_article_dimensions.website_article_publish_date AS website_article_publish_date,
        ga_article_dimensions.website_article_author AS website_article_author,
        ga_article_dimensions.website_article_word_count AS website_article_word_count,
        ga_article_dimensions.website_article_topics AS website_article_topics,
        ga_article_dimensions.website_article_topics_array AS website_article_topics_array,
        ga_article_performance.channel_name AS channel_name,
        ga_article_performance.channel_source_name AS channel_source_name,
        ga_article_performance.channel_source_type AS channel_source_type,
        ga_article_performance.utm_source AS utm_source,
        ga_article_performance.utm_medium AS utm_medium,
        ga_article_performance.utm_campaign AS utm_campaign,
        ga_article_performance.utm_content AS utm_content,
        ga_article_performance.utm_term AS utm_term,
        
        ga_article_performance.website_article_views AS website_article_views,
        ga_article_performance.website_article_read AS website_article_read,
        ga_article_performance.website_article_shares AS website_article_shares,
        ga_article_performance.website_article_engaged_time_total AS website_article_engaged_time_total
        
    FROM ga_article_performance
    
    LEFT JOIN ga_article_dimensions USING(website_article_id)

),

final AS (
  
    SELECT * 
    
    FROM join_ga_article_performance_and_dimensions
    
    WHERE website_article_title IS NOT NULL

)
      
SELECT * FROM final