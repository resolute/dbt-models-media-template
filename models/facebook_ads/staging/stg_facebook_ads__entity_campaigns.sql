{%- set source_account_ids = var('facebook_ads_ids') -%}

{{ config(enabled= (var('facebook_ads_ids'))|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'facebook_entity_campaigns') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast_dedupe AS (

    SELECT DISTINCT

        account_id,
        account_name,
        campaign_id,
        campaign_name,
        date,
        bid_strategy,
        buying_type,
        daily_budget,
        lifetime_budget,
        effective_status,
        configured_status,
        status,
        objective,
        spend_cap,
        start_time,
        stop_time,
        created_time,
        updated_time

    FROM source_data

),

rank_duplicate_campaign_ids AS (

    SELECT

        *,
        ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY updated_time DESC) AS rank_recent

    FROM rename_recast_dedupe

),

final AS (

    SELECT

        account_id,
        account_name,
        campaign_id,
        campaign_name,
        date,
        bid_strategy,
        buying_type,
        daily_budget,
        lifetime_budget,
        effective_status,
        configured_status,
        status,
        objective,
        spend_cap,
        start_time,
        stop_time,
        created_time,
        updated_time

    FROM rank_duplicate_campaign_ids

    WHERE rank_recent = 1

)

SELECT * FROM final