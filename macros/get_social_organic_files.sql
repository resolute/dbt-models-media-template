{% macro get_social_organic_files(table_type) %}

    {% set files = [] %}

    {% if var('facebook_organic_ids')|length > 0 %} 
        {% if table_type == 'followers' %}
            {% set _ = files.append(ref('facebook_organic__followers_daily')) %}
        {% elif table_type == 'posts' %}
            {% set _ = files.append(ref('facebook_organic__posts_lifetime')) %}
        {% endif %}
    {% endif %}

    {% if var('instagram_organic_ids')|length > 0 %} 
        {% if table_type == 'followers' %}
            {% set _ = files.append(ref('instagram_organic__followers_daily')) %}
        {% elif table_type == 'posts' %}
            {% set _ = files.append(ref('instagram_organic__posts_lifetime')) %}
        {% endif %}
    {% endif %}

    {% if var('twitter_organic_ids')|length > 0 %} 
        {% if table_type == 'followers' %}
            {% set _ = files.append(ref('twitter_organic__followers_daily')) %}
        {% elif table_type == 'posts' %}
            {% set _ = files.append(ref('twitter_organic__posts_lifetime')) %}
        {% endif %}
    {% endif %}

    {% if var('linkedin_organic_ids')|length > 0 %} 
        {% if table_type == 'followers' %}
            {% set _ = files.append(ref('linkedin_organic__followers_daily')) %}
        {% elif table_type == 'posts' %}
            {% set _ = files.append(ref('linkedin_organic__posts_lifetime')) %}
        {% endif %}
    {% endif %}

    {{ return(files) }}

{% endmacro %}