{%- set source_account_ids = get_account_ids('amazon ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'amazon_advertising_adgroups_sbv') }}

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
        campaign_budget,
        campaign_budget_type,
        campaign_status,
        adgroup_id,
        adgroup_name,
        CAST(NULL AS STRING) AS tactic,

        {#- General metrics -#}
        impressions,
        cost,
        clicks AS link_clicks,

        {#- Conversions -#}
        0 AS conv_attr_conv_1d,
        0 AS conv_attr_conv_7d,
        attr_conv_14d AS conv_amz_conv_14d,
        0 AS conv_attr_conv_30d,
        0 AS conv_attr_conv_1d_same_sku,
        0 AS conv_attr_conv_7d_same_sku,
        attr_conv_14d_same_sku AS conv_amz_conv_14d_same_sku,
        0 AS conv_attr_conv_30d_same_sku,
        0 AS conv_attr_sales_1d,
        0 AS conv_attr_sales_7d,
        attr_sales_14d AS conv_amz_sales_14d,
        0 AS conv_attr_sales_30d,
        0 AS conv_attr_sales_1d_same_sku,
        0 AS conv_attr_sales_7d_same_sku,
        attr_sales_14d_same_sku AS conv_amz_sales_14d_same_sku,
        0 AS conv_attr_sales_30d_same_sku,
        0 AS conv_attr_units_ordered_1d,
        0 AS conv_attr_units_ordered_7d,
        0 AS conv_attr_units_ordered_14d,
        0 AS conv_attr_units_ordered_30d

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