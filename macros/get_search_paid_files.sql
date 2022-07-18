{% macro get_search_paid_files(return_files=true) %}

    {% set files = [] %}

    {% if get_account_ids('google ads')|length > 0 %} 
        {% if return_files %}
            {% set _ = files.append(ref('prep_rollup_search_paid__campaigns_google_ads')) %}
        {% else %}
            {% set _ = files.append(true) %}
        {% endif %}
    {% endif %}

    {{ return(files) }}

{% endmacro %}