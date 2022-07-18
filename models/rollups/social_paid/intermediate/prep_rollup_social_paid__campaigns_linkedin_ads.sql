{{ config(enabled= get_account_ids('linkedin ads')|length > 0 is true) }}

{# Get a list of the columns from the upstream model #}
{%- set cols = adapter.get_columns_in_relation(ref('linkedin_ads__performance_daily')) -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('linkedin_ads__performance_daily') }}

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
        SUM(link_clicks) AS link_clicks,
        SUM(cost) AS cost,
        SUM(total_engagements) AS post_engagements,
        SUM(likes) AS post_reactions,
        SUM(comment) AS post_comments,
        SUM(share) AS post_shares,
        SUM(video_views) AS video_views,
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
        
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_id']) }} AS id,
        *
    
    FROM aggregate

)
  
SELECT * FROM final