{% macro get_social_organic_files(table_type) %}

    {% set files = [] %}

    {% if var('facebook_organic_ids')|length > 0 %} 
        {% if table_type == 'followers' %}
            {% set _ = files.append(ref('prep_rollup_social_organic__followers_daily_facebook')) %}
        {% elif table_type == 'posts' %}
            {% set _ = files.append(ref('prep_rollup_social_organic__posts_lifetime_facebook')) %}
        {% endif %}
    {% endif %}

    {% if var('instagram_organic_ids')|length > 0 %} 
        {% if table_type == 'followers' %}
            {% set _ = files.append(ref('prep_rollup_social_organic__followers_daily_instagram')) %}
        {% elif table_type == 'posts' %}
            {% set _ = files.append(ref('prep_rollup_social_organic__posts_lifetime_instagram')) %}
        {% endif %}
    {% endif %}

    {% if var('twitter_organic_ids')|length > 0 %} 
        {% if table_type == 'followers' %}
            {% set _ = files.append(ref('prep_rollup_social_organic__followers_daily_twitter')) %}
        {% elif table_type == 'posts' %}
            {% set _ = files.append(ref('prep_rollup_social_organic__posts_lifetime_twitter')) %}
        {% endif %}
    {% endif %}

    {% if var('linkedin_organic_ids')|length > 0 %} 
        {% if table_type == 'followers' %}
            {% set _ = files.append(ref('prep_rollup_social_organic__followers_daily_linkedin')) %}
        {% elif table_type == 'posts' %}
            {% set _ = files.append(ref('prep_rollup_social_organic__posts_lifetime_linkedin')) %}
        {% endif %}
    {% endif %}

    {{ return(files) }}

{% endmacro %}