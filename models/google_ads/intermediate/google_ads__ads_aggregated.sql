{{ config(enabled= (var('google_ads_ids'))|length > 0 is true) }}

WITH

aggregate AS (

{{
    aggregate_model(ref('stg_google_ads__ads'), 'SUM', ['id', 'ad_network_type_2'], ['avg_pos'])
}}

),

final AS (

    SELECT
        
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'ad_id', 'ad_network_type_1']) }} AS id,
        *
    
    FROM aggregate

)

SELECT * FROM final