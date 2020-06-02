{% set old_query %}
  select
    *
  from `yale-opac.google_analytics.stg_ga_alumni_events_page_performance`
  where date BETWEEN '2020-01-01' AND '2020-05-31'
{% endset %}

{% set new_query %}
  select
    *
    EXCEPT(id)
  from {{ ref('stg_google_analytics__alumni_events_page_performance_stitch') }}
  where date BETWEEN '2020-01-01' AND '2020-05-31'
{% endset %}

{{ audit_helper.compare_queries(
    a_query=old_query,
    b_query=new_query
) }}