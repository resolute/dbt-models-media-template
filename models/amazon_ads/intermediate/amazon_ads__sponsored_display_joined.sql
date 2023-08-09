{%- set source_account_ids = get_account_ids('amazon ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

ad_data AS (

    SELECT * FROM {{ ref('stg_amazon_ads__adgroups_sd') }}

),

campaign_data AS (

    SELECT * FROM {{ ref('stg_amazon_ads__entity_sd_campaigns') }}

),

join_ad_and_campaign AS (

    SELECT

        ad_data.*,
        campaign_data.start_date,
        campaign_data.end_date,
        CAST(NULL AS STRING) AS campaign_daily_budget,
        'sponsoredDisplay' AS campaign_type,
        campaign_data.campaign_serving_status,
        CAST(NULL AS STRING) AS campaign_bid_optimization,
        CAST(NULL AS STRING) AS campaign_premium_bid_adjustment,
        campaign_data.campaign_delivery_profile,
        campaign_data.campaign_cost_type,
        campaign_data.campaign_tactic,
        CAST(NULL AS STRING) AS campaign_targeting_type,
        CAST(NULL AS STRING) AS portfolio_id,
        CAST(NULL AS STRING) AS campaign_spending_policy

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