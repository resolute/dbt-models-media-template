{%- set source_account_ids = get_account_ids('google ads') -%}

{# Identify whether to enable this type of Google Ads model #}
{%- set enable_google_ads_model = true -%}
{%- set ev_enable_models = fromyaml(env_var('DBT_GOOGLE_ADS_MODELS_ENABLED', '')) -%}
{%- if ev_enable_models is not none and 'campaign' not in ev_enable_models -%}
    {%- set enable_google_ads_model = false -%}
{%- elif var('google_ads_models_enabled', [])|length > 0 is true and 'campaign' not in var('google_ads_models_enabled', []) -%}
    {%- set enable_google_ads_model = false -%}
{%- endif -%}

{{ config(enabled= source_account_ids|length > 0 is true and enable_google_ads_model is true and get_account_conversion_data_config('google ads')) }}

{# Identify the conversion metrics to include in this model #}
{%- set conversion_fields = []-%}
{%- set ev_conversion_metrics = fromyaml(env_var('DBT_GOOGLE_ADS_CONVERSION_METRICS', '')) -%}
{%- if ev_conversion_metrics is not none -%}
    {%- set conversion_fields = ev_conversion_metrics -%}
{%- else -%}
    {%- set conversion_fields = var('google_ads_conversion_metrics', ['all_conv', 'conversions', 'conversions_view_through', 'value_all_conv', 'value_conversions']) -%}
{%- endif -%}

{# Identify the conversion types to include in this model #}
{%- set conversion_type_fields = [] -%}
{%- set ev_conversion_types = fromyaml(env_var('DBT_GOOGLE_ADS_CONVERSION_TYPES', '')) -%}
{%- if ev_conversion_types is not none -%}
    {%- set conversion_type_fields = ev_conversion_types -%}
{%- else -%}
    {%- set conversion_type_fields = var('google_ads_conversion_types', ['action_name', 'action_category']) -%}
{%- endif -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('google_ads__campaign_conversions_pivot_prep') }}

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
        campaign_id,
        campaign_name,
        device

        {#- Conversions -#}

        {%- set conv_cat_values = dbt_utils.get_column_values(ref('google_ads__campaign_conversions_pivot_prep'), 'conversion_action_category_formatted', default=[]) -%}
        {%- if conv_cat_values != None and conv_cat_values|length > 0 and 'action_category' in conversion_type_fields -%}
        ,
            {%- for conversion_field in conversion_fields -%}

                {{- dbt_utils.pivot(
                    'conversion_action_category_formatted',
                    conv_cat_values,
                    True,
                    'sum',
                    '=',
                    'conv_gad_' ~ conversion_field ~ '_conversion_category_',
                    '',
                    conversion_field,
                    0,
                    True
                ) -}}{%- if not loop.last -%},{%- endif -%}

            {%- endfor -%}
        {%- endif %}

        {%- set conv_name_values = dbt_utils.get_column_values(ref('google_ads__ads_conversions_pivot_prep'), 'conversion_action_name_formatted', default=[]) -%}
        {%- if conv_name_values != None and conv_cat_values|length > 0 and 'action_name' in conversion_type_fields -%}
        ,
            {%- for conversion_field in conversion_fields -%}

                {{- dbt_utils.pivot(
                    'conversion_action_name_formatted',
                    conv_name_values,
                    True,
                    'sum',
                    '=',
                    'conv_gad_' ~ conversion_field ~ '_conversion_name_',
                    '',
                    conversion_field,
                    0,
                    True
                ) -}}{%- if not loop.last -%},{%- endif -%}

            {%- endfor -%}
        {%- endif %}
        
    FROM data

    GROUP BY 1,2,3,4,5,6,7,8,9,10

),

final AS (

    SELECT
        
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'campaign_id', 'device']) }} AS id,
        *
    
    FROM pivot_conversions

)
  
SELECT * FROM final