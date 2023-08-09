{%- set source_account_ids = get_account_ids('amazon ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'amazon_advertising_adgroups_hsa') }}

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
        CAST(NULL AS STRING) as tactic,

        {#- General metrics -#}
        impressions,
        cost,
        clicks AS link_clicks,

        {#- Conversions -#}
        attr_conv_14d AS conv_amz_conv_14d,
        attr_conv_14d_same_sku AS conv_amz_conv_14d_same_sku,
        attr_sales_14d AS conv_amz_sales_14d,
        attr_sales_14d_same_sku AS conv_amz_sales_14d_same_sku

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