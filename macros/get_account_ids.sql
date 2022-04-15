{% macro get_account_ids(platform) %}

    {% set account_ids = [] %}
    
    {% if platform == "facebook ads" %} 
        {% set ev = fromyaml(env_var('DBT_FACEBOOK_ADS_IDS', '')) %}
        {% if ev is not none %}
            {% set account_ids = ev %}
        {% else %}
            {% set account_ids = var('facebook_ads_ids', []) %}
        {% endif %}

    {% elif platform == "facebook organic" %}
        {% set ev = fromyaml(env_var('DBT_FACEBOOK_ORGANIC_IDS', '')) %}
        {% if ev is not none %}
            {% set account_ids = ev %}
        {% else %}
            {% set account_ids = var('facebook_organic_ids', []) %}
        {% endif %}

    {% elif platform == "google ads" %}
        {% set ev = fromyaml(env_var('DBT_GOOGLE_ADS_IDS', '')) %}
        {% if ev is not none %}
            {% set account_ids = ev %}
        {% else %}
            {% set account_ids = var('google_ads_ids', []) %}
        {% endif %}
    
    {% elif platform == "google analytics" %}
        {% set ev = fromyaml(env_var('DBT_GOOGLE_ANALYTICS_IDS', '')) %}
        {% if ev is not none %}
            {% set account_ids = ev %}
        {% else %}
            {% set account_ids = var('google_analytics_ids', []) %}
        {% endif %}
    
    {% elif platform == "google campaign manager" %}
        {% set ev = fromyaml(env_var('DBT_GOOGLE_CAMPAIGN_MANAGER_IDS', '')) %}
        {% if ev is not none %}
            {% set account_ids = ev %}
        {% else %}
            {% set account_ids = var('google_campaign_manager_ids', []) %}
        {% endif %}
    
    {% elif platform == "instagram organic" %}
        {% set ev = fromyaml(env_var('DBT_INSTAGRAM_ORGANIC_IDS', '')) %}
        {% if ev is not none %}
            {% set account_ids = ev %}
        {% else %}
            {% set account_ids = var('instagram_organic_ids', []) %}
        {% endif %}
    
    {% elif platform == "linkedin ads" %}
        {% set ev = fromyaml(env_var('DBT_LINKEDIN_ADS_IDS', '')) %}
        {% if ev is not none %}
            {% set account_ids = ev %}
        {% else %}
            {% set account_ids = var('linkedin_ads_ids', []) %}
        {% endif %}
    
    {% elif platform == "linkedin organic" %}
        {% set ev = fromyaml(env_var('DBT_LINKEDIN_ORGANIC_IDS', '')) %}
        {% if ev is not none %}
            {% set account_ids = ev %}
        {% else %}
            {% set account_ids = var('linkedin_organic_ids', []) %}
        {% endif %}
    
    {% elif platform == "pinterest ads" %}
        {% set ev = fromyaml(env_var('DBT_PINTEREST_ADS_IDS', '')) %}
        {% if ev is not none %}
            {% set account_ids = ev %}
        {% else %}
            {% set account_ids = var('pinterest_ads_ids', []) %}
        {% endif %}
    
    {% elif platform == "twitter ads" %}
        {% set ev = fromyaml(env_var('DBT_TWITTER_ADS_IDS', '')) %}
        {% if ev is not none %}
            {% set account_ids = ev %}
        {% else %}
            {% set account_ids = var('twitter_ads_ids', []) %}
        {% endif %}
    
    {% elif platform == "twitter organic" %}
        {% set ev = fromyaml(env_var('DBT_TWITTER_ORGANIC_IDS', '')) %}
        {% if ev is not none %}
            {% set account_ids = ev %}
        {% else %}
            {% set account_ids = var('twitter_organic_ids', []) %}
        {% endif %}
    
    {% elif platform == "youtube organic" %}
        {% set ev = fromyaml(env_var('DBT_YOUTUBE_ORGANIC_IDS', '')) %}
        {% if ev is not none %}
            {% set account_ids = ev %}
        {% else %}
            {% set account_ids = var('youtube_organic_ids', []) %}
        {% endif %}

    {% endif %}
    
    {{ return(account_ids) }}

{% endmacro %}