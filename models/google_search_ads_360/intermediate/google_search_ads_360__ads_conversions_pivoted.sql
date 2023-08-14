{%- set source_account_ids = get_account_ids('google search ads 360') -%}

{{ config(enabled= source_account_ids|length > 0 is true and get_account_conversion_data_config('google search ads 360')) }}


{# Identify the conversion metrics to include in this model #}
{%- set conversion_fields = []-%}
{%- set ev_conversion_metrics = fromyaml(env_var('DBT_GOOGLE_SEARCH_ADS_360_CONVERSION_METRICS', '')) -%}
{%- if ev_conversion_metrics is not none -%}
    {%- set conversion_fields = ev_conversion_metrics -%}
{%- else -%}
    {%- set conversion_fields = var('google_search_ads_360_conversion_metrics', ['actions', 'weighted_actions', 'transactions', 'revenue']) -%}
{%- endif -%}

{# Identify the conversion types to include in this model #}
{%- set conversion_type_fields = [] -%}
{%- set ev_conversion_types = fromyaml(env_var('DBT_GOOGLE_SEARCH_ADS_360_CONVERSION_TYPES', '')) -%}
{%- if ev_conversion_types is not none -%}
    {%- set conversion_type_fields = ev_conversion_types -%}
{%- else -%}
    {%- set conversion_type_fields = var('google_search_ads_360_conversion_types', ['floodlight_activity', 'floodlight_group']) -%}
{%- endif -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('google_search_ads_360__ads_conversions_pivot_prep') }}

),

pivot_conversions AS (

    SELECT
    
        {# Dimensions -#}
        data_source,
        channel_source_name,
        channel_source_type,
        channel_name,
        account_id,
        account_name,
        date,
        advertiser_id,
        advertiser_name,
        engine_account_name,
        engine_account_type,
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        ad_id,
        ad_name,
        ad_labels

        {#- Conversions -#}

        {%- set conv_cat_values = dbt_utils.get_column_values(ref('google_search_ads_360__ads_conversions_pivot_prep'), 'floodlight_group_formatted') -%}
        {%- if conv_cat_values != None -%}
        ,
            {%- for conversion_field in conversion_fields -%}

                {{- dbt_utils.pivot(
                    'floodlight_group_formatted',
                    conv_cat_values,
                    True,
                    'sum',
                    '=',
                    'conv_gsa_' ~ conversion_field ~ '_floodlight_group_',
                    '',
                    conversion_field,
                    0,
                    True
                ) -}}{%- if not loop.last -%},{%- endif -%}

            {%- endfor -%}
        {%- endif %}

        {%- set conv_name_values = dbt_utils.get_column_values(ref('google_search_ads_360__ads_conversions_pivot_prep'), 'floodlight_activity_formatted') -%}
        {%- if conv_name_values != None -%}
        ,
            {%- for conversion_field in conversion_fields -%}

                {{- dbt_utils.pivot(
                    'floodlight_activity_formatted',
                    conv_name_values,
                    True,
                    'sum',
                    '=',
                    'conv_gsa_' ~ conversion_field ~ '_floodlight_activity_',
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
        
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'advertiser_id', 'engine_account_name', 'campaign_id', 'ad_group_id', 'ad_id']) }} AS id,
        *
    
    FROM pivot_conversions

)
  
SELECT * FROM final