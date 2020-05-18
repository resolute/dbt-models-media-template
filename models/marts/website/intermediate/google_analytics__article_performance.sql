WITH

data AS (
  
    SELECT * FROM {{ ref('stg_google_analytics__article_performance') }}

),
  
general_definitions AS (
    
    SELECT
    
        *,
        'Google Analytics' AS data_source
        
    FROM data

),
  
data_cleaning AS (

    SELECT
        
        data_source,
        account_id,
        account_name,
        date,
        TRIM(website_article_id) AS website_article_id,
        
        TRIM(
            CASE
                WHEN STRPOS(website_page_path, "?") > 0 THEN SUBSTR(website_page_path, 0, STRPOS(website_page_path, "?") - 1)
                WHEN STRPOS(website_page_path, "#") > 0 THEN SUBSTR(website_page_path, 0, STRPOS(website_page_path, "#") - 1)
                ELSE website_page_path
            END
        ) AS website_page_path,
        
        TRIM(website_channel_group) AS website_channel_group,
        TRIM(LOWER(utm_source)) AS utm_source,
        TRIM(LOWER(utm_medium)) AS utm_medium,
        TRIM(LOWER(utm_campaign)) AS utm_campaign,
        TRIM(LOWER(utm_content)) AS utm_content,
        TRIM(LOWER(utm_term)) AS utm_term,
        
        website_article_views,
        website_article_read,
        website_article_shares,
        website_article_engaged_time_total
        
     FROM general_definitions

),

filter_out_not_set_article_ids AS (

    SELECT *
    
    FROM data_cleaning
    
    WHERE website_article_id != '(not set)'

),

define_channel_name AS (

    SELECT
    
        * EXCEPT(website_channel_group),
        
        CASE
            WHEN website_channel_group = '(Other)' AND utm_source = 'yaletoday' AND utm_medium = 'email' THEN 'Email'
            WHEN website_channel_group = '(Other)' AND utm_source = 'email' AND utm_medium = 'abstract' THEN 'Email'
            --WHEN website_channel_group = '(Other)' AND utm_source = 'email' AND utm_medium = 'abstract' THEN 'Paid Social'
            WHEN website_channel_group = 'Social' THEN 'Organic Social'
            ELSE website_channel_group
        END AS channel_name        
    
    FROM filter_out_not_set_article_ids

),

define_channel_dimensions AS (

    SELECT
    
        *,
        
        CASE
            WHEN channel_name = 'Email' THEN 'Email'
            WHEN channel_name = 'Organic Social' AND REGEXP_CONTAINS(utm_source, r'(facebook)') THEN 'Facebook'
            WHEN channel_name = 'Organic Social' AND REGEXP_CONTAINS(utm_source, r'(^t.co$|twitter)') THEN 'Twitter'
            WHEN channel_name = 'Organic Social' AND REGEXP_CONTAINS(utm_source, r'(instagram)') THEN 'Instagram'
            WHEN channel_name = 'Organic Social' AND REGEXP_CONTAINS(utm_source, r'(linkedin)') THEN 'LinkedIn'
            WHEN channel_name = 'Organic Social' AND REGEXP_CONTAINS(utm_source, r'(youtube)') THEN 'YouTube'
            WHEN channel_name = 'Organic Social' AND REGEXP_CONTAINS(utm_source, r'(pinterest)') THEN 'Pinterest'
            WHEN channel_name = 'Organic Social' AND REGEXP_CONTAINS(utm_source, r'(meetup)') THEN 'Meetup'
            WHEN channel_name = '(Other)' THEN '(Other)'
            ELSE utm_source
        END AS channel_source_name,
        
        CASE
            WHEN channel_name IN ('Email', 'Paid Social', 'Paid Search') THEN 'Paid'
            WHEN channel_name = '(Other)' THEN '(Other)'
            ELSE 'Organic'
        END AS channel_source_type,
    
    FROM define_channel_name

),

final AS (

    SELECT
    
        data_source,
        account_id,
        account_name,
        date,
        website_article_id,
        website_page_path,
        channel_name,
        channel_source_name,
        channel_source_type,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        
        SUM(website_article_views) AS website_article_views,
        SUM(website_article_read) AS website_article_read,
        SUM(website_article_shares) AS website_article_shares,
        SUM(website_article_engaged_time_total) AS website_article_engaged_time_total
    
    FROM define_channel_dimensions
    
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14

)
      
SELECT * FROM final