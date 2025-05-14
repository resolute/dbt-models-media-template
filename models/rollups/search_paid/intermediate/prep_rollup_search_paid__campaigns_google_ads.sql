{# Identify whether to enable this type of Google Ads model #}
{%- set enable_google_ads_model = true -%}
{%- set ev_enable_models = fromyaml(env_var('DBT_GOOGLE_ADS_MODELS_ENABLED', '')) -%}
{%- if ev_enable_models is not none and 'campaign' not in ev_enable_models -%}
    {%- set enable_google_ads_model = false -%}
{%- elif var('google_ads_models_enabled', [])|length > 0 is true and 'campaign' not in var('google_ads_models_enabled', []) -%}
    {%- set enable_google_ads_model = false -%}
{%- endif -%}

{{ config(enabled= get_account_ids('google ads')|length > 0 is true and enable_google_ads_model is true) }}

{# Get a list of the columns from the upstream model #}
{%- set cols = adapter.get_columns_in_relation(ref('google_ads__campaign_performance_daily')) -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('google_ads__campaign_performance_daily') }}

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