{% macro get_search_paid_files() %}

    {% set files = [] %}

    {% if var('google_ads_ids')|length > 0 %} 
    {% set _ = files.append(ref('prep_rollup_search_paid__campaigns_google_ads')) %}
    {% endif %}

    {{ return(files) }}

{% endmacro %}