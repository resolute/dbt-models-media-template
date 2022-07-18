{% macro get_social_organic_files(table_type, return_files=true) %}

    {% set files = [] %}

    {% if get_account_ids('facebook organic')|length > 0 %} 
        {% if return_files %}
            {% if table_type == 'followers' %}
                {% set _ = files.append(ref('prep_rollup_social_organic__followers_daily_facebook')) %}
            {% elif table_type == 'posts' %}
                {% set _ = files.append(ref('prep_rollup_social_organic__posts_lifetime_facebook')) %}
            {% endif %}
        {% else %}
            {% set _ = files.append(true) %}
        {% endif %}
    {% endif %}

    {% if get_account_ids('instagram organic')|length > 0 %} 
        {% if return_files %}
            {% if table_type == 'followers' %}
                {% set _ = files.append(ref('prep_rollup_social_organic__followers_daily_instagram')) %}
            {% elif table_type == 'posts' %}
                {% set _ = files.append(ref('prep_rollup_social_organic__posts_lifetime_instagram')) %}
            {% endif %}
        {% else %}
            {% set _ = files.append(true) %}
        {% endif %}
    {% endif %}

    {% if get_account_ids('twitter organic')|length > 0 %} 
        {% if return_files %}
            {% if table_type == 'followers' %}
                {% set _ = files.append(ref('prep_rollup_social_organic__followers_daily_twitter')) %}
            {% elif table_type == 'posts' %}
                {% set _ = files.append(ref('prep_rollup_social_organic__posts_lifetime_twitter')) %}
            {% endif %}
        {% else %}
            {% set _ = files.append(true) %}
        {% endif %}
    {% endif %}

    {% if get_account_ids('linkedin organic')|length > 0 %} 
        {% if return_files %}
            {% if table_type == 'followers' %}
                {% set _ = files.append(ref('prep_rollup_social_organic__followers_daily_linkedin')) %}
            {% elif table_type == 'posts' %}
                {% set _ = files.append(ref('prep_rollup_social_organic__posts_lifetime_linkedin')) %}
            {% endif %}
        {% else %}
            {% set _ = files.append(true) %}
        {% endif %}
    {% endif %}

    {{ return(files) }}

{% endmacro %}