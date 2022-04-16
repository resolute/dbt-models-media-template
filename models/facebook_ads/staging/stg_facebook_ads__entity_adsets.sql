{%- set source_account_ids = get_account_ids('facebook ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'facebook_entity_adsets') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

final AS (

    SELECT
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

    QUALIFY ROW_NUMBER() OVER (PARTITION BY adset_id ORDER BY __insert_date DESC) = 1

)

SELECT * FROM final