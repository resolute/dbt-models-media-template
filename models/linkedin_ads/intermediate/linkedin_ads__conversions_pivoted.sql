{# Identify the conversion metrics to include in this model #}
{%- set conversion_fields = [
    'conversions',
    'value_conversions',
    'conversions_click_through',
    'conversions_view_through',
    'viral_conversions',
    'viral_conversions_click_through',
    'viral_conversions_view_through'
    ]-%}

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
        campaign_group_id,
        campaign_group_name,
        campaign_id,
        campaign_name,
        campaign_type

        {#- Conversions -#}

        {%- set conv_cat_values = dbt_utils.get_column_values(ref('linkedin_ads__conversions_pivot_prep'), 'conversion_type_formatted') -%}
        {%- if conv_cat_values != None -%}
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

        {%- set conv_name_values = dbt_utils.get_column_values(ref('linkedin_ads__conversions_pivot_prep'), 'conversion_name_formatted') -%}
        {%- if conv_name_values != None -%}
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

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12

),

final AS (

    SELECT
        
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_id']) }} AS id,
        *
    
    FROM pivot_conversions

)
  
SELECT * FROM final