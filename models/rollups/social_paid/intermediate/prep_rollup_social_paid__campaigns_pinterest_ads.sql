{{ config(enabled= get_account_ids('pinterest ads')|length > 0 is true) }}

{# Get a list of the columns from the upstream model #}
{%- set cols = adapter.get_columns_in_relation(ref('pinterest_ads__pins_performance_daily')) -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('pinterest_ads__pins_performance_daily') }}

),

aggregate AS (

    SELECT
    
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        date,
        campaign_id,
        campaign_name,
        
        SUM(paid_impressions + earned_impressions) AS impressions,
        SUM(paid_clicks) AS link_clicks,
        SUM(cost) AS cost,
        SUM(paid_engagements + earned_engagements) AS post_engagements,
        0 AS post_reactions,
        0 AS post_comments,
        0 AS post_shares,
        SUM(video_starts_paid + video_starts_earned) AS video_views,
        SUM(video_p25_watched_paid + video_p25_watched_earned) AS video_p25_watched,
        SUM(video_p50_watched_paid + video_p50_watched_earned) AS video_p50_watched,
        SUM(video_p75_watched_paid + video_p75_watched_earned) AS video_p75_watched,
        SUM(video_completions_paid + video_completions_earned) AS video_completions

        {%- for col in cols if "conv_" in col.column -%}

        ,
        SUM({{ col.column }}) AS {{ col.column }}

        {%- endfor %}
        
     FROM data

     GROUP BY 1,2,3,4,5,6,7,8,9

),

final AS (

    SELECT
        
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'campaign_id']) }} AS id,
        *
    
    FROM aggregate

)
  
SELECT * FROM final