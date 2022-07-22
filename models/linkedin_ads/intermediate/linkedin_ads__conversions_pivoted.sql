{%- set source_account_ids = get_account_ids('linkedin ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true and get_account_conversion_data_config('linkedin ads')) }}

{# Identify the conversion metrics to include in this model #}
{%- set conversion_fields = []-%}
{%- set ev_conversion_metrics = fromyaml(env_var('DBT_LINKEDIN_ADS_CONVERSION_METRICS', '')) -%}
{%- if ev_conversion_metrics is not none -%}
    {%- set conversion_fields = ev_conversion_metrics -%}
{%- else -%}
    {%- set conversion_fields = var('linkedin_ads_conversion_metrics', ['conversions', 'conversions_click_through', 'conversions_view_through', 'viral_conversions', 'viral_conversions_click_through', 'viral_conversions_view_through']) -%}
{%- endif -%}

{# Identify the conversion types to include in this model #}
{%- set conversion_type_fields = [] -%}
{%- set ev_conversion_types = fromyaml(env_var('DBT_LINKEDIN_ADS_CONVERSION_TYPES', '')) -%}
{%- if ev_conversion_types is not none -%}
    {%- set conversion_type_fields = ev_conversion_types -%}
{%- else -%}
    {%- set conversion_type_fields = var('linkedin_ads_conversion_types', ['conversion_name', 'conversion_type']) -%}
{%- endif -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('linkedin_ads__conversions_pivot_prep') }}

),

pivot_conversions AS (

    SELECT
    
        {# Dimensions -#}
        
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        date,
        campaign_id,
        campaign_name,
        campaign_type,
        creative_id,
        creative_name,
        creative_text,
        creative_title,
        creative_status,
        destination_url

        {#- Conversions -#}

        {%- set conv_cat_values = dbt_utils.get_column_values(ref('linkedin_ads__conversions_pivot_prep'), 'conversion_type_formatted', default=[]) -%}
        {%- if conv_cat_values != None and conv_cat_values|length > 0 and 'conversion_type' in conversion_type_fields -%}
        ,
            {%- for conversion_field in conversion_fields -%}

                {{- dbt_utils.pivot(
                    'conversion_type_formatted',
                    conv_cat_values,
                    True,
                    'sum',
                    '=',
                    'conv_li_' ~ conversion_field ~ '_conversion_type_',
                    '',
                    conversion_field,
                    0,
                    True
                ) -}}{%- if not loop.last -%},{%- endif -%}

            {%- endfor -%}
        {%- endif %}

        {%- set conv_name_values = dbt_utils.get_column_values(ref('linkedin_ads__conversions_pivot_prep'), 'conversion_name_formatted', default=[]) -%}
        {%- if conv_name_values != None and conv_cat_values|length > 0 and 'conversion_name' in conversion_type_fields -%}
        ,
            {%- for conversion_field in conversion_fields -%}

                {{- dbt_utils.pivot(
                    'conversion_name_formatted',
                    conv_name_values,
                    True,
                    'sum',
                    '=',
                    'conv_li_' ~ conversion_field ~ '_conversion_name_',
                    '',
                    conversion_field,
                    0,
                    True
                ) -}}{%- if not loop.last -%},{%- endif -%}

            {%- endfor -%}
        {%- endif %}
        
    FROM data

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

),

final AS (

    SELECT
        
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'creative_id']) }} AS id,
        *
    
    FROM pivot_conversions

)
  
SELECT * FROM final