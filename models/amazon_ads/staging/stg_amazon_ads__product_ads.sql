{%- set source_account_ids = get_account_ids('amazon ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'amazon_advertising_product_ads') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        date,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        adgroup_id,
        adgroup_name,
        ad_id,
        asin,
        sku,

        {#- General metrics -#}
        impressions,
        cost,
        clicks AS link_clicks,

        {#- Conversions -#}
        attributed_conversions_1d AS conv_attr_conv_1d,
        attributed_conversions_7d AS conv_attr_conv_7d,
        attributed_conversions_14d AS conv_attr_conv_14d,
        attributed_conversions_30d AS conv_attr_conv_30d,
        attributed_conversions_1d_same_sku AS conv_attr_conv_1d_same_sku,
        attributed_conversions_7d_same_sku AS conv_attr_conv_7d_same_sku,
        attributed_conversions_14d_same_sku AS conv_attr_conv_14d_same_sku,
        attributed_conversions_30d_same_sku AS conv_attr_conv_30d_same_sku,
        attributed_sales_1d AS conv_attr_sales_1d,
        attributed_sales_7d AS conv_attr_sales_7d,
        attributed_sales_14d AS conv_attr_sales_14d,
        attributed_sales_30d AS conv_attr_sales_30d,
        attributed_sales_1d_same_sku AS conv_attr_sales_1d_same_sku,
        attributed_sales_7d_same_sku AS conv_attr_sales_7d_same_sku,
        attributed_sales_14d_same_sku AS conv_attr_sales_14d_same_sku,
        attributed_sales_30d_same_sku AS conv_attr_sales_30d_same_sku,
        attributed_units_ordered_1d AS conv_attr_units_ordered_1d,
        attributed_units_ordered_7d AS conv_attr_units_ordered_7d,
        attributed_units_ordered_14d AS conv_attr_units_ordered_14d,
        attributed_units_ordered_30d AS conv_attr_units_ordered_30d,
        attributed_units_ordered_1d_same_sku AS conv_attr_units_ordered_1d_same_sku,
        attributed_units_ordered_7d_same_sku AS conv_attr_units_ordered_7d_same_sku,
        attributed_units_ordered_14d_same_sku AS conv_attr_units_ordered_14d_same_sku,
        attributed_units_ordered_30d_same_sku AS conv_attr_units_ordered_30d_same_sku

        -- Excluded fields --
        /*
        __insert_date,
        date_yyyymmdd
        */

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['date', 'campaign_id', 'adgroup_id', 'ad_id' 'asin', 'sku']) }} AS id,
        'Amazon Ads' AS data_source,
        'Amazon' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Display' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final