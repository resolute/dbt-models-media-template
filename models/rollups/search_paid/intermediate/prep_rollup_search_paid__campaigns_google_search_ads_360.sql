{{ config(enabled= get_account_ids('google search ads 360')|length > 0 is true) }}

{# Get a list of the columns from the upstream model #}
{%- set cols = adapter.get_columns_in_relation(ref('google_search_ads_360__ads_performance_daily')) -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('google_search_ads_360__ads_performance_daily') }}

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
        0 AS video_views,
        0 AS video_p25_watched,
        0 AS video_p50_watched,
        0 AS video_p75_watched,
        0 AS video_completions

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