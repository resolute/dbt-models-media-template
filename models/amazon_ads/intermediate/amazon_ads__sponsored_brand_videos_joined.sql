{%- set source_account_ids = get_account_ids('amazon ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

ad_data AS (

    SELECT * FROM {{ ref('stg_amazon_ads__adgroups_sbv') }}

),

campaign_data AS (

    SELECT * FROM {{ ref('stg_amazon_ads__entity_sbv_campaigns') }}

),

join_ad_and_campaign AS (

    SELECT

        ad_data.*,
        campaign_data.start_date,
        campaign_data.end_date,
        CAST(NULL AS STRING) AS campaign_daily_budget,
        'sponsoredBrandVideos' AS campaign_type,
        campaign_data.campaign_serving_status,
        CAST(NULL AS STRING) AS campaign_bid_optimization,
        CAST(NULL AS STRING) AS campaign_premium_bid_adjustment,
        CAST(NULL AS STRING) AS campaign_delivery_profile,
        CAST(NULL AS STRING) AS campaign_cost_type,
        CAST(NULL AS STRING) as campaign_tactic,
        CAST(NULL AS STRING) AS campaign_targeting_type,
        campaign_data.portfolio_id,
        CAST(NULL AS STRING) AS campaign_spending_policy,
        0 AS conv_amz_conv_1d,
        0 AS conv_amz_conv_7d,
        0 AS conv_amz_conv_30d,
        0 AS conv_amz_conv_1d_same_sku,
        0 AS conv_amz_conv_7d_same_sku,
        0 AS conv_amz_conv_30d_same_sku,
        0 AS conv_amz_sales_1d,
        0 AS conv_amz_sales_7d,
        0 AS conv_amz_sales_30d,
        0 AS conv_amz_sales_1d_same_sku,
        0 AS conv_amz_sales_7d_same_sku,
        0 AS conv_amz_sales_30d_same_sku,
        0 AS conv_amz_units_ordered_1d,
        0 AS conv_amz_units_ordered_7d,
        0 AS conv_amz_units_ordered_14d,
        0 AS conv_amz_units_ordered_30d

    FROM ad_data

    LEFT JOIN campaign_data 
        ON(ad_data.campaign_id = campaign_data.campaign_id)

),

final AS (

    SELECT

        *

    FROM join_ad_and_campaign

)

SELECT * FROM final