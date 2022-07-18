{%- set source_account_ids = get_account_ids('twitter ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

{# Get a list of the columns from the upstream model #}
{%- set cols = adapter.get_columns_in_relation(source('improvado', 'twitter_promoted_tweets_with_cards')) -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'twitter_promoted_tweets_with_cards') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        account_id,
        account_name,
        date,
        campaign_id,
        campaign_web_id,
        campaign_name,
        created_at AS campaign_create_at,
        line_item_id AS ad_group_id,
        line_item_web_id AS ad_group_web_id,
        line_item_name AS ad_group_name,
        line_item_status AS ad_group_status,
        objective,
        promoted_tweet_id AS ad_id,
        promoted_tweet_web_id AS ad_web_id,
        promoted_tweet_status AS ad_status,
        card_uri AS ad_card_uri,
        card_name AS ad_card_name,
        text AS ad_text,
        url AS ad_url,
        destination_url AS ad_destination_url,
        currency,
        
        {#- General metrics -#}
        impressions,
        qualified_impressions,
        spend AS cost,
        url_clicks AS link_clicks,
        clicks,

        {# Engagement metrics -#}
        engagements,
        likes,
        replies,
        retweets,
        card_engagements,
        media_views,
        media_engagements,
        follows,
        app_clicks,
        billed_engagements,
        billed_charge_local_micro,

        {#- Video metrics -#}
        video_total_views AS video_views,
        video_views_25 AS video_p25_watched,
        video_views_50 AS video_p50_watched,
        video_views_75 AS video_p75_watched,
        video_views_100 AS video_completions,
        video_cta_clicks,
        video_content_starts,
        video_mrc_views,
        video_3s100pct_views

        {#- Conversions -#}

        {#- Loop through each conversion and rename column -#}
        {%- for col in cols if modules.re.match("conversion_|mobile_conversion_", col.column) -%}

        ,
        {{ col.column }} AS conv_tw_{{ col.column|replace("conversion_", "") }}

        {%- endfor  %}

        -- Excluded fields --
        /*
        */

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_web_id', 'ad_group_web_id', 'ad_web_id']) }} AS id,
        'Twitter Paid' AS data_source,
        'Twitter' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Social' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final