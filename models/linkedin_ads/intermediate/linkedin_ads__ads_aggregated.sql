{{ config(enabled= (var('linkedin_ads_ids'))|length > 0 is true) }}

WITH

aggregate AS (

{{
    aggregate_model(ref('stg_linkedin_ads__creatives'), 'SUM', ['id'], [])
}}

),

final AS (

    SELECT
        
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'creative_id']) }} AS id,
        *
    
    FROM aggregate

)

SELECT * FROM final
