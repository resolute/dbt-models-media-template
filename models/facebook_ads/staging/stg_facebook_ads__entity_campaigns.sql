{%- set source_account_ids = get_account_ids('facebook ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'facebook_entity_campaigns') }}

    WHERE REPLACE(account_id, 'act_', '') IN (SELECT REPLACE(x, 'act_', '') FROM UNNEST({{ source_account_ids }}) AS x)

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

    FROM source_data

    QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY __insert_date DESC) = 1

)

SELECT * FROM final