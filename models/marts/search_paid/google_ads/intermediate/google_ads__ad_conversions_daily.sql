{# Identify the conversion metrics to include in this model #}
{%- set conversion_fields = [
    'conversions',
    'conversion_value'
    ]-%}

WITH

data AS (
  
    SELECT * FROM {{ ref('prep_google_ads__ad_conversions_daily') }}

),
  
general_definitions AS (

    SELECT
    
        *,
        'Google Ads' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name
  
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
        customer_id,
        campaign_id,
        campaign_name,
        adset_id,
        adset_name,
        ad_id,
        ad_name,
        description,
        ad_network_type_1,
        headline_1 AS headline1,
        headline_2 AS headline2,
        creative_urls AS destination_url,

        {%- for conversion_field in conversion_fields -%}

            {{- dbt_utils.pivot(
                'conversion_category_formatted',
                dbt_utils.get_column_values(ref('prep_google_ads__ad_conversions_daily'), 'conversion_category_formatted'),
                True,
                'sum',
                '=',
                'conv_gad_' ~ conversion_field ~ '_conversion_category_',
                '',
                conversion_field,
                0,
                True
            ) -}}{%- if not loop.last -%},{%- endif -%}

        {%- endfor -%},

        {%- for conversion_field in conversion_fields -%}

            {{- dbt_utils.pivot(
                'conversion_name_formatted',
                dbt_utils.get_column_values(ref('prep_google_ads__ad_conversions_daily'), 'conversion_name_formatted'),
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
        
    FROM general_definitions

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19

),

final AS (

    SELECT
        
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_id', 'adset_id', 'ad_id', 'ad_network_type_1']) }} AS id,
        *
    
    FROM pivot_conversions

)
  
SELECT * FROM final