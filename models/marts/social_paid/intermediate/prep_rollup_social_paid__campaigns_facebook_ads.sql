{# Get a list of the columns from the upstream model #}
{%- set cols = adapter.get_columns_in_relation(ref('facebook_ads__performance_daily')) -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('facebook_ads__performance_daily') }}

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
        
        SUM(impressions) AS impressions,
        SUM(outbound_clicks) AS link_clicks,
        SUM(cost) AS cost,
        SUM(post_engagement_total) AS post_engagements,
        SUM(post_reactions) AS post_reactions,
        SUM(post_comments) AS post_comments,
        SUM(post_shares) AS post_shares,
        SUM(video_view_3s) AS video_views,
        SUM(video_p25_watched) AS video_p25_watched,
        SUM(video_p50_watched) AS video_p50_watched,
        SUM(video_p75_watched) AS video_p75_watched,
        SUM(video_completions) AS video_completions

        {%- for col in cols if "conv_" in col.column -%}

        ,
        SUM({{ col.column }}) AS {{ col.column }}

        {%- endfor %}
        
     FROM data

     GROUP BY 1,2,3,4,5,6,7,8,9

),

final AS (

    SELECT
        
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'channel_source_name', 'campaign_id']) }} AS id,
        *
    
    FROM aggregate

)
  
SELECT * FROM final