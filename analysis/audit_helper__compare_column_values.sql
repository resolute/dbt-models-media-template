{# in dbt Develop #}

{% set old_etl_relation_query %}
    select
        {{ dbt_utils.surrogate_key(['collected_date', 'account_id', 'website_article_id', 'website_article_title', 'website_page_path', 'website_article_publish_date', 'website_article_author', 'website_article_word_count', 'website_article_topics']) }} AS id,
        *
    from `yale-opac.google_analytics.stg_ga_article_dimensions_combined`
    where collected_date BETWEEN '2020-02-15' AND DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 1 DAY)
{% endset %}

{% set new_etl_relation_query %}
    select
        *
    from {{ ref('stg_google_analytics__article_dimensions_stitch') }}
    where collected_date BETWEEN '2020-02-15' AND DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 1 DAY)
{% endset %}

{% set audit_query = audit_helper.compare_column_values(
    a_query=old_etl_relation_query,
    b_query=new_etl_relation_query,
    primary_key="id",
    column_to_compare="website_article_id"
) %}

{% set audit_results = run_query(audit_query) %}

{% if execute %}
{% do audit_results.print_table() %}
{% endif %}