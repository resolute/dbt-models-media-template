{% set old_query %}
  select
    *
    EXCEPT(website_article_title)
  from `yale-opac.google_analytics.TEST_stg_ga_article_dimensions_combined`
  where collected_date BETWEEN '2020-02-15' AND DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 1 DAY)
{% endset %}

{% set new_query %}
  select
    *
    EXCEPT(id,website_article_title)
  from {{ ref('stg_google_analytics__article_dimensions') }}
  where collected_date BETWEEN '2020-02-15' AND DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 1 DAY)
{% endset %}

{{ audit_helper.compare_queries(
    a_query=old_query,
    b_query=new_query
) }}