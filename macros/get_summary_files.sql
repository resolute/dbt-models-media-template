{% macro get_summary_files(return_files=true) %}

    {% set files = [] %}

    {% if get_social_paid_files(return_files=false)|length > 0 %}
        {% if return_files %}
            {% set _ = files.append(ref('prep_rollup_paid_media__campaigns_social_paid')) %}
        {% else %}
            {% set _ = files.append(true) %}
        {% endif %}
    {% endif %}

    {% if get_display_files(return_files=false)|length > 0 %}
        {% if return_files %}
            {% set _ = files.append(ref('prep_rollup_paid_media__campaigns_display')) %}
        {% else %}
            {% set _ = files.append(true) %}
        {% endif %}
    {% endif %}

    {% if get_search_paid_files(return_files=false)|length > 0 %}
        {% if return_files %}
            {% set _ = files.append(ref('prep_rollup_paid_media__campaigns_search_paid')) %}
        {% else %}
            {% set _ = files.append(true) %}
        {% endif %}
    {% endif %}

    {{ return(files) }}

{% endmacro %}