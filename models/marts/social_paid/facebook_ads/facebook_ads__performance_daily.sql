{#- Get a list of the Facebook Ads conversions that are active for the Facebook Ads accounts -#}
{%- set conversions = dbt_utils.get_query_results_as_dict("select * from" ~ ref('facebook_ads__account_conversions')) -%}
{%- set active_conversions = [] -%}
{%- for column, value in conversions.items() -%}
    {%- if value[0] != 0.0 -%}
        {% do active_conversions.append(column) -%}
    {%- endif -%}
{%- endfor -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_facebook_ads__creative') }}

),

final AS (

    SELECT
    
        {# Dimensions -#}
        id,
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        date,
        campaign_id,
        campaign_name,
        campaign_type,
        adset_id,
        adset_name,
        ad_id,
        ad_name,
        creative_id,
        creative_name,
        ad_objective,
        ad_publication_date,
        ad_type,
        body,
        name,
        description,
        caption,
        call_to_action_type,
        format_option,
        preview_shareable_link,
        instagram_permalink_url,
        creative_link,
        image,
        image_url,
        website_destination_url,
        creative_destination_url,
        video_creative_destination_url,
        buying_type,
        lead_gen_form_id,
        object_story_id,
        effective_object_story_id,
        effective_status,
        
        {#- Impression and Cost metrics -#}
        reach,
        impressions,
        cost,

        {#- Click Metrics -#}
        link_clicks,
        unique_inline_link_clicks,
        outbound_clicks,
        unique_outbound_clicks,
        clicks,

        {#- Engagement metrics -#}
        post_engagement_total,
        inline_post_engagement,
        post_reactions,
        post_likes,
        post_comments,
        post_shares,
        post_saves,
        post_story_total,
        page_engagement_total,
        page_story_total,
        page_likes,
        app_engagement_total,
        app_story_total,
        app_use,
        mobile_app_install,
        instagram_profile_engagement_total,

        {#- Video metrics -#}
        video_views,
        video_play,
        video_play_actions_view_value,
        video_p25_watched,
        video_p50_watched,
        video_p75_watched,
        video_p95_watched,
        video_completions,
        video_10_sec_watched_actions,
        video_thru_play,
        video_30_sec_watched_actions,
        video_avg_time_watched_actions,
        video_average_play_time_count,
        video_avg_percent_watched_actions,

        {#- Other metrics -#}
        views,
        relevance_score,
        purchase,
        purchase_value

        {#- Conversions -#}

        {#- Loop through each active conversion -#}
        {%- for conv in active_conversions -%}
        
        ,
        {{ conv }}

        {%- endfor %}
        
     FROM data

)
  
SELECT * FROM final