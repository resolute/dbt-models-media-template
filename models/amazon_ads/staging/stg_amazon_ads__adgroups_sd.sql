{%- set source_account_ids = get_account_ids('amazon ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'amazon_advertising_adgroups_sd') }}

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
        campaign_budget_type,
        campaign_status,
        adgroup_id,
        adgroup_name,
        tactic,

        {#- General metrics -#}
        impressions,
        cost,
        clicks AS link_clicks,

        {#- Conversions -#}
        attr_conv_1d AS conv_amz_conv_1d,
        attr_conv_7d AS conv_amz_conv_7d,
        attr_conv_14d AS conv_amz_conv_14d,
        attr_conv_30d AS conv_amz_conv_30d,
        attr_conv_1d_same_sku AS conv_amz_conv_1d_same_sku,
        attr_conv_7d_same_sku AS conv_amz_conv_7d_same_sku,
        attr_conv_14d_same_sku AS conv_amz_conv_14d_same_sku,
        attr_conv_30d_same_sku AS conv_amz_conv_30d_same_sku,
        attr_sales_1d AS conv_amz_sales_1d,
        attr_sales_7d AS conv_amz_sales_7d,
        attr_sales_14d AS conv_amz_sales_14d,
        attr_sales_30d AS conv_amz_sales_30d,
        attr_sales_1d_same_sku AS conv_amz_sales_1d_same_sku,
        attr_sales_7d_same_sku AS conv_amz_sales_7d_same_sku,
        attr_sales_14d_same_sku AS conv_amz_sales_14d_same_sku,
        attr_sales_30d_same_sku AS conv_amz_sales_30d_same_sku,
        attr_units_ordered_1d AS conv_amz_units_ordered_1d,
        attr_units_ordered_7d AS conv_amz_units_ordered_7d,
        attr_units_ordered_14d AS conv_amz_units_ordered_14d,
        attr_units_ordered_30d AS conv_amz_units_ordered_30d

        -- Excluded fields --
        /*
        __insert_date,
        date_yyyymmdd
        */

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['date', 'campaign_id', 'adgroup_id']) }} AS id,
        'Amazon Ads' AS data_source,
        'Amazon' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Display' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final