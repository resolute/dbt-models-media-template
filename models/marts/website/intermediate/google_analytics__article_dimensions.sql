WITH

data AS (
  
    SELECT * FROM {{ ref('stg_google_analytics__article_dimensions') }}

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
        TRIM(website_article_id) AS website_article_id,
        TRIM(website_article_title) AS website_article_title,
        
        TRIM(
            CASE
                WHEN STRPOS(website_page_path, "?") > 0 THEN SUBSTR(website_page_path, 0, STRPOS(website_page_path, "?") - 1)
                WHEN STRPOS(website_page_path, "#") > 0 THEN SUBSTR(website_page_path, 0, STRPOS(website_page_path, "#") - 1)
                ELSE website_page_path
            END
        ) AS website_page_path,
        
        TRIM(website_article_publish_date) AS website_article_publish_date,
        TRIM(website_article_author) AS website_article_author,
        TRIM(website_article_word_count) AS website_article_word_count,
        TRIM(website_article_topics) AS website_article_topics,
        collected_date
        
     FROM general_definitions

),

filter_out_translate_page_paths AS (

    SELECT *
    
    FROM data_cleaning
    
    WHERE REGEXP_CONTAINS(website_page_path, r'^(\/translate|\/transpage)') = FALSE

),

filter_out_incorrect_publish_dates AS (

    SELECT *
    
    FROM filter_out_translate_page_paths
    
    WHERE SAFE_CAST(website_article_publish_date AS DATE) <= collected_date

),

identify_recent_data AS (

    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY website_article_id 
                           ORDER BY collected_date DESC, website_article_topics DESC, website_article_publish_date DESC) 
                           AS most_recent_data_rank
    
    FROM filter_out_incorrect_publish_dates

),

/*identify_duplicates_based_on_id AS (

    SELECT
    
        *,
        ROW_NUMBER() OVER (PARTITION BY website_article_id 
                           ORDER BY website_article_publish_date DESC, website_article_topics DESC) 
                           AS dedupe_rows
    
    FROM filter_out_translate_page_paths

),*/

filter_to_recent_data AS (

    SELECT * EXCEPT(most_recent_data_rank)
    
    FROM identify_recent_data
    
    WHERE most_recent_data_rank = 1

),

get_distinct_articles AS (

    SELECT DISTINCT *
    
    FROM filter_to_recent_data

),

create_topics_array_field AS (

    SELECT 
    
        *,
        
        SPLIT(website_article_topics) AS website_article_topics_array
    
    FROM get_distinct_articles

),

trim_topics_array_values AS (

    SELECT 
    
        * EXCEPT(website_article_topics_array),
        
        ARRAY(
            SELECT TRIM(x) FROM UNNEST(website_article_topics_array) AS x
        ) AS website_article_topics_array
    
    FROM create_topics_array_field

),

final AS (

    SELECT 
    
        * EXCEPT(website_article_topics, website_article_topics_array),
        
        ARRAY_TO_STRING(
            ARRAY(SELECT DISTINCT x FROM UNNEST(website_article_topics_array) AS x), ', ') 
            
        AS website_article_topics,
        
        ARRAY(
            SELECT DISTINCT x FROM UNNEST(website_article_topics_array) AS x
        ) AS website_article_topics_array
    
    FROM trim_topics_array_values

)
      
SELECT * FROM final