{%- set source_account_ids = get_account_ids('amazon ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'amazon_advertising_entity_sp_campaigns') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        date,
        start_date,
        account_id,
        account_name,
        campaign_id,
        name AS campaign_name,
        daily_budget AS campaign_daily_budget,
        campaign_type,
        targeting_type AS campaign_targeting_type,
        state AS campaign_serving_status,
        premium_bid_adjustment AS campaign_premium_bid_adjustment,
        portfolio_id

        -- Excluded fields --
        /*
        __insert_date,
        date_yyyymmdd
        */

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['date', 'campaign_id']) }} AS id,
        'Amazon Ads' AS data_source,
        'Amazon' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Display' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final