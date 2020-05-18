WITH

source_data AS (
  
    SELECT * FROM {{ source('google_analytics', 'ga_article_dimensions_combined') }}

),

final AS (

    SELECT DISTINCT
    
        CAST(View_ID AS STRING) AS account_id,
        CONCAT('Yale News (UA-991898-10) -> ', View) AS account_name,
        CAST(Custom_dimension_3 AS STRING) AS website_article_id,
        Custom_dimension_4 AS website_article_title,
        Page_path AS website_page_path,
        Custom_dimension_5 AS website_article_publish_date,
        Custom_dimension_1 AS website_article_author,
        Custom_dimension_6 AS website_article_word_count,
        Custom_dimension_2 AS website_article_topics,
        Date AS collected_date
    
    FROM source_data

)

SELECT * FROM final