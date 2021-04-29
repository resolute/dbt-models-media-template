{% macro get_social_paid_files() %}

    {% set files = [] %}

    {% if var('facebook_ads_ids')|length > 0 %} 
    {% set _ = files.append(ref('prep_rollup_social_paid__campaigns_facebook_ads')) %}
    {% endif %}

    {% if var('linkedin_ads_ids')|length > 0 %} 
    {% set _ = files.append(ref('prep_rollup_social_paid__campaigns_linkedin_ads')) %}
    {% endif %}

    {% if var('twitter_ads_ids')|length > 0 %} 
    {% set _ = files.append(ref('prep_rollup_social_paid__campaigns_twitter_ads')) %}
    {% endif %}

    {% if var('pinterest_ads_ids')|length > 0 %} 
    {% set _ = files.append(ref('prep_rollup_social_paid__campaigns_pinterest_ads')) %}
    {% endif %}

    {{ return(files) }}

{% endmacro %}