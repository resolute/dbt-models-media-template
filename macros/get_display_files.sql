{% macro get_display_files(return_files=true) %}

    {% set files = [] %}

    {% if get_account_ids('google campaign manager')|length > 0 %}
        {% if return_files %}
            {% set _ = files.append(ref('prep_rollup_display__campaigns_google_campaign_manager')) %}
        {% else %}
            {% set _ = files.append(true) %}
        {% endif %}
    {% endif %}

    {{ return(files) }}

{% endmacro %}