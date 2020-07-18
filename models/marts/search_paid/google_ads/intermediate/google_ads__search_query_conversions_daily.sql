{# Identify the conversion metrics to include in this model #}
{%- set conversion_fields = [
    'all_conv',
    'value_all_conv',
    'conversions',
    'value_conversions',
    'conversions_view_through'
    ]-%}

WITH

data AS (
  
    SELECT * FROM {{ ref('prep_google_ads__search_query_conversions_daily') }}

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
        ad_network_type_1,
        search_term,
        query_match_type,

        {#- Conversions -#}

        {%- for conversion_field in conversion_fields -%}

            {{- dbt_utils.pivot(
                'conversion_category_formatted',
                dbt_utils.get_column_values(ref('prep_google_ads__search_query_conversions_daily'), 'conversion_category_formatted'),
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
                dbt_utils.get_column_values(ref('prep_google_ads__search_query_conversions_daily'), 'conversion_name_formatted'),
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
        
    FROM data

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17

),

final AS (

    SELECT
        
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_id', 'ad_group_id', 'keyword_id', 'search_term', 'ad_network_type_1']) }} AS id,
        *
    
    FROM pivot_conversions

)
  
SELECT * FROM final