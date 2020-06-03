{% set old_query %}
  select
    {{ dbt_utils.surrogate_key(['collected_date', 'account_id', 'website_article_id', 'website_article_title', 'website_page_path', 'website_article_publish_date', 'website_article_author', 'website_article_word_count', 'website_article_topics']) }} AS id,
    *
  from `yale-opac.google_analytics.stg_ga_article_dimensions_combined`
  where collected_date BETWEEN '2020-02-15' AND DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 1 DAY)
{% endset %}

{% set new_query %}
  select
    *
  from {{ ref('stg_google_analytics__article_dimensions_stitch') }}
  where collected_date BETWEEN '2020-02-15' AND DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 1 DAY)
{% endset %}

{{ audit_helper.compare_queries(
    a_query=old_query,
    b_query=new_query,
    primary_key="id"
) }}