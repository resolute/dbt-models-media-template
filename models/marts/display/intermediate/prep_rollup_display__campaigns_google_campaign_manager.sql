{# Get a list of the columns from the upstream model #}
{%- set cols = adapter.get_columns_in_relation(ref('google_campaign_manager__performance_daily')) -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('google_campaign_manager__performance_daily') }}

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
        SUM(rich_media_video_plays) AS video_views,
        SUM(video_first_quartile_completions) AS video_p25_watched,
        SUM(video_midpoints) AS video_p50_watched,
        SUM(video_third_quartile_completions) AS video_p75_watched,
        SUM(rich_media_video_completions) AS video_completions

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