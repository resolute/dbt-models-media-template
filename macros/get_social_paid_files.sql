{% macro get_social_paid_files() %}

    {% set files = [] %}

    {% if get_account_ids('facebook ads')|length > 0 %} 
    {% set _ = files.append(ref('prep_rollup_social_paid__campaigns_facebook_ads')) %}
    {% endif %}

    {% if get_account_ids('linkedin ads')|length > 0 %} 
    {% set _ = files.append(ref('prep_rollup_social_paid__campaigns_linkedin_ads')) %}
    {% endif %}

    {% if get_account_ids('twitter ads')|length > 0 %} 
    {% set _ = files.append(ref('prep_rollup_social_paid__campaigns_twitter_ads')) %}
    {% endif %}

    {% if get_account_ids('pinterest ads')|length > 0 %} 
    {% set _ = files.append(ref('prep_rollup_social_paid__campaigns_pinterest_ads')) %}
    {% endif %}

    {{ return(files) }}

{% endmacro %}