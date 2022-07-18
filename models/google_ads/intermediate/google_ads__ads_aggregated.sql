{%- set source_account_ids = get_account_ids('google ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

aggregate AS (

{{
    aggregate_model(ref('stg_google_ads__ads'), 'SUM', ['id'])
}}

),

final AS (

    SELECT
        
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'ad_id', 'ad_network_type']) }} AS id,
        *
    
    FROM aggregate

)

SELECT * FROM final