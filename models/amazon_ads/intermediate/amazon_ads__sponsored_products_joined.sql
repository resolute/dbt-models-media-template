{%- set source_account_ids = get_account_ids('amazon ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

ad_data AS (

    SELECT * FROM {{ ref('stg_amazon_ads__adgroups_sp') }}

),

campaign_data AS (

    SELECT * FROM {{ ref('stg_amazon_ads__entity_sp_campaigns') }}

),

join_ad_and_campaign AS (

    SELECT

        ad_data.*,
        campaign_data.start_date,
        CAST(NULL AS STRING) AS end_date,
        campaign_data.campaign_daily_budget,
        campaign_data.campaign_type,
        campaign_data.campaign_serving_status,
        CAST(NULL AS STRING) AS campaign_bid_optimization,
        campaign_data.campaign_premium_bid_adjustment,
        CAST(NULL AS STRING) AS campaign_delivery_profile,
        CAST(NULL AS STRING) AS campaign_cost_type,
        CAST(NULL AS STRING) AS campaign_tactic,
        campaign_data.campaign_targeting_type,        
        campaign_data.portfolio_id,
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