{%- set source_account_ids = get_account_ids('facebook ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'facebook_entity_ads') }}

    WHERE REPLACE(account_id, 'act_', '') IN (SELECT REPLACE(x, 'act_', '') FROM UNNEST({{ source_account_ids }}) AS x)

),

final AS (

    SELECT
        account_id,
        account_name,
        DATE(updated_time) AS date

    FROM source_data

    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY DATE(updated_time) DESC) = 1

)

SELECT * FROM final