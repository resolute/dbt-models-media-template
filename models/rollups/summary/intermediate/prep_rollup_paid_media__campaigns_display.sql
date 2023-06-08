{%- set relations_list = get_display_files(return_files=false) -%}

{%- if relations_list|length < 1 -%}
{{ config(enabled=false) }}
{%- endif -%}

{# Get a list of the columns from the upstream model #}
{%- set cols = adapter.get_columns_in_relation(ref('rollup_display__campaigns')) -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('rollup_display__campaigns') }}

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
        
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'campaign_id']) }} AS id,
        *
    
    FROM aggregate

)
  
SELECT * FROM final