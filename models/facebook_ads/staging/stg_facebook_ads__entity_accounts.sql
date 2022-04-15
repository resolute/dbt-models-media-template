{%- set source_account_ids = get_account_ids('facebook ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'facebook_entity_ads') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast_dedupe_drop AS (

    SELECT DISTINCT

        account_id,
        account_name,
        date

    FROM source_data

),

rank_duplicate_account_ids AS (

    SELECT

        *,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY date DESC) AS rank_recent

    FROM rename_recast_dedupe_drop

),

final AS (

    SELECT

        account_id,
        account_name,
        date

    FROM rank_duplicate_account_ids

    WHERE rank_recent = 1

)

SELECT * FROM final