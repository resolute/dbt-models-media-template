{% macro get_social_paid_files(return_files=true) %}

    {% set files = [] %}

    {% if get_account_ids('facebook ads')|length > 0 %}
        {% if return_files %}
            {% set _ = files.append(ref('prep_rollup_social_paid__campaigns_facebook_ads')) %}
        {% else %}
            {% set _ = files.append(true) %}
        {% endif %}
    {% endif %}

    {% if get_account_ids('linkedin ads')|length > 0 %}
        {% if return_files %}
            {% set _ = files.append(ref('prep_rollup_social_paid__campaigns_linkedin_ads')) %}
        {% else %}
            {% set _ = files.append(true) %}
        {% endif %}
    {% endif %}

    {% if get_account_ids('twitter ads')|length > 0 %}
        {% if return_files %}
            {% set _ = files.append(ref('prep_rollup_social_paid__campaigns_twitter_ads')) %}
        {% else %}
            {% set _ = files.append(true) %}
        {% endif %}
    {% endif %}

    {% if get_account_ids('pinterest ads')|length > 0 %}
        {% if return_files %}
            {% set _ = files.append(ref('prep_rollup_social_paid__campaigns_pinterest_ads')) %}
        {% else %}
            {% set _ = files.append(true) %}
        {% endif %}
    {% endif %}

    {{ return(files) }}

{% endmacro %}