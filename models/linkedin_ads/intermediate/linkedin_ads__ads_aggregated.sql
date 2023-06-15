{%- set source_account_ids = get_account_ids('linkedin ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

aggregate AS (

{{
    aggregate_model(ref('stg_linkedin_ads__creatives'), 'SUM', ['id'], [])
}}

),

final AS (

    SELECT
        
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'creative_id']) }} AS id,
        *
    
    FROM aggregate

)

SELECT * FROM final
