{# Identify the conversion metrics to include in this model #}
{%- set conversion_fields = [
    'conversions',
    'post_click_conversions',
    'conversion_value'
    ]-%}

WITH

data AS (
  
    SELECT * FROM {{ ref('prep_linkedin_ads__conversions_daily') }}

),
  
general_definitions AS (

    SELECT
    
        *,
        'LinkedIn Paid' AS data_source,
        'LinkedIn' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Social' AS channel_name
  
    FROM data
    
),

pivot_conversions AS (

    SELECT
    
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
        campaign_type,

        {%- for conversion_field in conversion_fields -%}

            {{- dbt_utils.pivot(
                'conversion_type_formatted',
                dbt_utils.get_column_values(ref('prep_linkedin_ads__conversions_daily'), 'conversion_type_formatted'),
                True,
                'sum',
                '=',
                'conv_li_' ~ conversion_field ~ '_conversion_type_',
                '',
                conversion_field,
                0,
                True
            ) -}}{%- if not loop.last -%},{%- endif -%}

        {%- endfor -%},

        {%- for conversion_field in conversion_fields -%}

            {{- dbt_utils.pivot(
                'conversion_name_formatted',
                dbt_utils.get_column_values(ref('prep_linkedin_ads__conversions_daily'), 'conversion_name_formatted'),
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
        
    FROM general_definitions

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12

),

final AS (

    SELECT
        
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_id']) }} AS id,
        *
    
    FROM pivot_conversions

)
  
SELECT * FROM final