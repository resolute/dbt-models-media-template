{%- set source_account_ids = get_account_ids('facebook ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'facebook_entity_ads') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

final AS (

    SELECT
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

    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_id ORDER BY __insert_date DESC) = 1

)

SELECT * FROM final