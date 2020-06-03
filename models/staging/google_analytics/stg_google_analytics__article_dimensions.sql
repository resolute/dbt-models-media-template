WITH

source_data AS (
  
    SELECT * FROM {{ source('google_analytics_stitch_yale_news', 'ga_article_dimensions_stitch') }}

),

recast_rename AS (
  
    SELECT DISTINCT

        profile_id AS account_id,
        CONCAT('Yale News (UA-991898-10) -> ', '1 - Yale News') AS account_name,
        ga_dimension3 AS website_article_id,
        ga_dimension4 AS website_article_title,
        ga_pagepath AS website_page_path,
        ga_dimension5 AS website_article_publish_date,
        ga_dimension1 AS website_article_author,
        ga_dimension6 AS website_article_word_count,
        ga_dimension2 AS website_article_topics,
        DATE(start_date) AS collected_date
    
    FROM source_data

),

final AS (

    SELECT

        {{ dbt_utils.surrogate_key(['collected_date', 'account_id', 'website_article_id', 'website_article_title', 'website_page_path', 'website_article_publish_date', 'website_article_author', 'website_article_word_count', 'website_article_topics']) }} AS id,
        *
    
    FROM recast_rename

)

SELECT * FROM final