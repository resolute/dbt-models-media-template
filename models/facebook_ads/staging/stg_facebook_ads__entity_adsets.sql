{%- set source_account_ids = var('facebook_ads_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'facebook_entity_adsets') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast_dedupe AS (

    SELECT DISTINCT

        account_id,
        account_name,
        adset_id,
        name AS adset_name,
        campaign_id,
        date,
        created_time,
        updated_time,
        destination_type,
        effective_status,
        optimization_goal,
        start_time,
        end_time,
        targeting,
        daily_budget,
        lifetime_budget

    FROM source_data

),

rank_duplicate_adset_ids AS (

    SELECT

        *,
        ROW_NUMBER() OVER (PARTITION BY adset_id ORDER BY updated_time DESC) AS rank_recent

    FROM rename_recast_dedupe

),

final AS (

    SELECT

        account_id,
        account_name,
        adset_id,
        adset_name,
        campaign_id,
        date,
        created_time,
        updated_time,
        destination_type,
        effective_status,
        optimization_goal,
        start_time,
        end_time,
        targeting,
        daily_budget,
        lifetime_budget

    FROM rank_duplicate_adset_ids

    WHERE rank_recent = 1

)

SELECT * FROM final