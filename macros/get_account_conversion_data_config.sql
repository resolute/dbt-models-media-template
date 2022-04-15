{% macro get_account_conversion_data_config(platform) %}

    {% set config_value = false %}
    
    {% if platform == "google ads" %} 
        {% set ev = fromyaml(env_var('DBT_GOOGLE_ADS_CONVERSIONS_ENABLED', '')) %}
        {% if ev is not none %}
            {% set config_value = ev %}
        {% else %}
            {% set config_value = var('google_ads_conversions_enabled', false) %}
        {% endif %}

    {% elif platform == "google campaign manager" %}
        {% set ev = fromyaml(env_var('DBT_GOOGLE_CAMPAIGN_MANAGER_CONVERSIONS_ENABLED', '')) %}
        {% if ev is not none %}
            {% set config_value = ev %}
        {% else %}
            {% set config_value = var('google_campaign_manager_conversions_enabled', false) %}
        {% endif %}

    {% elif platform == "linkedin ads" %}
        {% set ev = fromyaml(env_var('DBT_LINKEDIN_ADS_CONVERSIONS_ENABLED', '')) %}
        {% if ev is not none %}
            {% set config_value = ev %}
        {% else %}
            {% set config_value = var('linkedin_ads_conversions_enabled', false) %}
        {% endif %}

    {% endif %}

    {{ return(config_value) }}

{% endmacro %}