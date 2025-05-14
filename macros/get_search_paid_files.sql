{% macro get_search_paid_files(return_files=true) %}

    {% set files = [] %}

    {# Identify whether to enable this type of Google Ads model #}
    {%- set enable_google_ads_model = true -%}
    {%- set ev_enable_models = fromyaml(env_var('DBT_GOOGLE_ADS_MODELS_ENABLED', '')) -%}
    {%- if ev_enable_models is not none and 'campaign' not in ev_enable_models -%}
        {%- set enable_google_ads_model = false -%}
    {%- elif var('google_ads_models_enabled', [])|length > 0 is true and 'campaign' not in var('google_ads_models_enabled', []) -%}
        {%- set enable_google_ads_model = false -%}
    {%- endif -%}

    {% if get_account_ids('bing ads')|length > 0 %} 
        {% if return_files %}
            {% set _ = files.append(ref('prep_rollup_search_paid__campaigns_bing_ads')) %}
        {% else %}
            {% set _ = files.append(true) %}
        {% endif %}
    {% endif %}

    {% if get_account_ids('google ads')|length > 0 and enable_google_ads_model is true %} 
        {% if return_files %}
            {% set _ = files.append(ref('prep_rollup_search_paid__campaigns_google_ads')) %}
        {% else %}
            {% set _ = files.append(true) %}
        {% endif %}
    {% endif %}

    {{ return(files) }}

{% endmacro %}