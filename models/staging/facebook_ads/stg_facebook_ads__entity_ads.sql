{%- set source_account_ids = var('facebook_ads_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'facebook_entity_ads') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast_dedupe AS (

    SELECT DISTINCT

        account_id,
        account_name,
        ad_id,
        name AS ad_name,
        adset_id,
        campaign_id,
        creative_id,
        date,
        created_time,
        updated_time,
        effective_status,
        status

    FROM source_data

),

rank_duplicate_ad_ids AS (

    SELECT

        *,
        ROW_NUMBER() OVER (PARTITION BY ad_id ORDER BY updated_time DESC) AS rank_recent

    FROM rename_recast_dedupe

),

final AS (

    SELECT

        account_id,
        account_name,
        ad_id,
        ad_name,
        adset_id,
        campaign_id,
        creative_id,
        date,
        created_time,
        updated_time,
        effective_status,
        status

    FROM rank_duplicate_ad_ids

    WHERE rank_recent = 1

)

SELECT * FROM final
