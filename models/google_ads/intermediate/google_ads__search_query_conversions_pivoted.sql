{%- set source_account_ids = get_account_ids('google ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true and get_account_conversion_data_config('google ads')) }}

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
  
    SELECT * FROM {{ ref('google_ads__search_query_conversions_pivot_prep') }}

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
        customer_id,
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        keyword_id,
        keyword_name,
        search_term,
        search_term_match_type,
        ad_network_type

        {#- Conversions -#}

        {%- set conv_cat_values = dbt_utils.get_column_values(ref('google_ads__search_query_conversions_pivot_prep'), 'conversion_action_category_formatted', default=[]) -%}
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

        {%- set conv_name_values = dbt_utils.get_column_values(ref('google_ads__search_query_conversions_pivot_prep'), 'conversion_action_name_formatted', default=[]) -%}
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

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17

),

final AS (

    SELECT
        
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'ad_group_id', 'keyword_id', 'search_term', 'ad_network_type', 'search_term_match_type']) }} AS id,
        *
    
    FROM pivot_conversions

)
  
SELECT * FROM final