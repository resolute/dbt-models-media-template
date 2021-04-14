{% macro get_summary_files() %}

    {% set files = [] %}

    {% if get_social_paid_files()|length > 0 %} 
    {% set _ = files.append(ref('prep_rollup_paid_media__campaigns_social_paid')) %}
    {% endif %}

    {% if get_display_files()|length > 0 %} 
    {% set _ = files.append(ref('prep_rollup_paid_media__campaigns_display')) %}
    {% endif %}

    {% if get_search_paid_files()|length > 0 %} 
    {% set _ = files.append(ref('prep_rollup_paid_media__campaigns_search_paid')) %}
    {% endif %}

    {{ return(files) }}

{% endmacro %}