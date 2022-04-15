{% macro get_display_files() %}

    {% set files = [] %}

    {% if get_account_ids('google campaign manager')|length > 0 %} 
    {% set _ = files.append(ref('prep_rollup_display__campaigns_google_campaign_manager')) %}
    {% endif %}

    {{ return(files) }}

{% endmacro %}