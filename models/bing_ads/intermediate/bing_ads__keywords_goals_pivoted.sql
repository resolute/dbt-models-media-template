{%- set source_account_ids = get_account_ids('bing ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true and get_account_conversion_data_config('bing ads')) }}

{# Identify the conversion metrics to include in this model #}
{%- set conversion_fields = []-%}
{%- set ev_conversion_metrics = fromyaml(env_var('DBT_BING_ADS_CONVERSION_METRICS', '')) -%}
{%- if ev_conversion_metrics is not none -%}
    {%- set conversion_fields = ev_conversion_metrics -%}
{%- else -%}
    {%- set conversion_fields = var('bing_ads_conversion_metrics', ['all_conversions', 'conversions', 'all_conversions_qualified', 'value_conversions']) -%}
{%- endif -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('bing_ads__keywords_goals_pivot_prep') }}

),

pivot_conversions AS (

    SELECT
    
        {# -- Dimensions -- #}
        data_source,
        channel_source_name,
        channel_source_type,
        channel_name,
        account_id,
        account_name,
        account_number,
        date,
        campaign_id,
        campaign_name,
        adset_id,
        adset_name,
        keyword_id,
        keyword_name,
        bid_match_type,
        destination_url,
        current_max_cpc,
        ad_distribution

        {#- Conversions -#}

        {%- set conv_name_values = dbt_utils.get_column_values(ref('bing_ads__keywords_goals_pivot_prep'), 'goal_formatted', default=[]) -%}
        {%- if conv_name_values != None and conv_name_values|length > 0 -%}
        ,
            {%- for conversion_field in conversion_fields -%}

                {{- dbt_utils.pivot(
                    'goal_formatted',
                    conv_name_values,
                    True,
                    'sum',
                    '=',
                    'conv_bingads_' ~ conversion_field ~ '_goal_',
                    '',
                    conversion_field,
                    0,
                    True
                ) -}}{%- if not loop.last -%},{%- endif -%}

            {%- endfor -%}
        {%- endif %}
        
    FROM data

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18

),

final AS (

    SELECT
        
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'adset_id', 'ad_distribution', 'keyword_id']) }} AS id,
        *
    
    FROM pivot_conversions

)

SELECT * FROM final