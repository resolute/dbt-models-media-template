{# Identify the standard Facebook Ads conversion metrics to include in this model #}
{%- set standard_conversions = [
    'add_payment_info',
    'add_to_cart',
    'add_to_wishlist',
    'complete_registration',
    'donate_total',
    'donate_total_value',
    'initiate_checkout',
    'landing_page_view',
    'lead',
    'purchase',
    'purchase_value',
    'search',
    'search_total',
    'start_trial_total_value',
    'submit_application_total',
    'subscribe_total',
    'subscribe_total_value',
    'view_content'
    ]-%}

{# Format Facebook Ads custom conversions list to mimic the Improvado custom conversion columns #}
{%- set custom_conversions_formatted = var('facebook_ads_custom_conversions')|map('lower')|map('replace',' ','_')|list -%}

{# Get a list of the columns from the upstream model #}
{%- set cols = adapter.get_columns_in_relation(ref('stg_facebook_ads__creative')) -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_facebook_ads__creative') }}

),
  
extract_utm_params AS (

    SELECT
    
        *,
        REGEXP_EXTRACT(website_destination_url,r'[?&]utm_source=([^&]+)') as utm_source,
        REGEXP_EXTRACT(website_destination_url,r'[?&]utm_medium=([^&]+)') as utm_medium,
        REGEXP_EXTRACT(website_destination_url,r'[?&]utm_campaign=([^&]+)') as utm_campaign,
        REGEXP_EXTRACT(website_destination_url,r'[?&]utm_content=([^&]+)') as utm_content,
        REGEXP_EXTRACT(website_destination_url,r'[?&]utm_term=([^&]+)') as utm_term
    
    FROM data
    
),
  
general_definitions AS (

    SELECT
    
        *,
        'Facebook Paid' AS data_source,

        CASE
            WHEN instagram_permalink_url IS NULL THEN 'Facebook'
            WHEN instagram_permalink_url IS NOT NULL THEN 'Instagram'
        END AS channel_source_name,

        'Paid' AS channel_source_type,
        'Paid Social' AS channel_name
  
    FROM extract_utm_params
    
),

rename_columns_and_set_defaults AS (

    SELECT
    
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
        adset_id,
        adset_name,
        ad_id,
        ad_name,
        creative_id,
        creative_name,
        objective AS ad_objective,
        publication_date AS ad_publish_date,
        object_type AS ad_type,
        creative_link AS ad_permalink_url,
        image_url AS ad_thumbnail_url,
        body AS ad_message,
        website_destination_url AS ad_destination_link_url,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        
        reach AS impressions_unique,
        impressions AS impressions,
        outbound_clicks AS link_clicks,
        spend AS cost,
        post_engagement AS post_engagements,
        post_reactions,
        comments AS post_comments,
        shares AS post_shares,
        page_likes,
        video_view_3s,
        video_30_sec_watched_actions,
        video_p25_watched_actions,
        video_p50_watched_actions,
        video_p75_watched_actions,
        video_p95_watched_actions,
        video_p100_watched_actions,
        video_avg_time_watched_actions,
        video_play_actions_view_value

        {#- Loop through each standard conversion and rename column to include attribution model used -#}
        {%- for conv in standard_conversions -%}
        
        ,
        {{ conv }} AS conv_fb_28c_1v_{{ conv }}

        {%- endfor -%}

        {#- Loop through each custom conversion from the dbt variable. Then loop through each upstream model column to check if the custom conversion is contained in any of the columns. If there is a match then return the upstream column and rename column to include attribution model used -#}
        {%- for conv in custom_conversions_formatted -%}

            {%- for col in cols if conv in col.column -%}
        
        ,
        {{ col.column }} AS conv_fb_28c_1v_custom_{{ col.column|replace("dynamic_", "") }}

            {%- endfor -%}

        {% endfor %}
        
     FROM general_definitions

),

final AS (

    SELECT
        
        *,
        DATE_DIFF(date, ad_publish_date, DAY) AS ad_age_in_days
    
    FROM rename_columns_and_set_defaults

)
  
SELECT * FROM final