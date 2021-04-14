{{ config(enabled=var('google_campaign_manager_conversions_enabled')) }}

{# Identify the conversion metrics to include in this model #}
{%- set conversion_fields = [
    'conversions',
    'conversions_click_through',
    'conversions_view_through',
    'value_conversions',
    'value_conversions_click_through',
    'value_conversions_view_through'
    ]-%}

WITH

data AS (
  
    SELECT * FROM {{ ref('google_campaign_manager__ads_conversions_pivot_prep') }}

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
        advertiser_id,
        advertiser_name,
        campaign_id,
        campaign_name,
        site_id,
        site,
        placement_id,
        placement,
        ad_id,
        ad,
        creative_id,
        creative,
        ad_type,
        creative_pixel_size,
        placement_size

        {#- Conversions -#}

        {%- set conv_cat_values = dbt_utils.get_column_values(ref('google_campaign_manager__ads_conversions_pivot_prep'), 'activity_group_formatted') -%}
        {%- if conv_cat_values != None -%}
        ,
            {%- for conversion_field in conversion_fields -%}

                {{- dbt_utils.pivot(
                    'activity_group_formatted',
                    conv_cat_values,
                    True,
                    'sum',
                    '=',
                    'conv_gcm_' ~ conversion_field ~ '_activity_group_',
                    '',
                    conversion_field,
                    0,
                    True
                ) -}}{%- if not loop.last -%},{%- endif -%}

            {%- endfor -%}
        {%- endif %}

        {%- set conv_name_values = dbt_utils.get_column_values(ref('google_campaign_manager__ads_conversions_pivot_prep'), 'activity_formatted') -%}
        {%- if conv_name_values != None -%}
        ,
            {%- for conversion_field in conversion_fields -%}

                {{- dbt_utils.pivot(
                    'activity_formatted',
                    conv_name_values,
                    True,
                    'sum',
                    '=',
                    'conv_gcm_' ~ conversion_field ~ '_activity_',
                    '',
                    conversion_field,
                    0,
                    True
                ) -}}{%- if not loop.last -%},{%- endif -%}

            {%- endfor -%}
        {%- endif %}

    FROM data

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22

),

final AS (

    SELECT

        {{ dbt_utils.surrogate_key(['date', 'account_id', 'advertiser_id', 'site_id', 'placement_id', 'placement_size', 'ad_id', 'creative_id']) }} AS id,
        *

    FROM pivot_conversions

)

SELECT * FROM final