{{ config(enabled= (var('google_ads_ids'))|length > 0 is true) }}

WITH

aggregate AS (

{{
    aggregate_model(ref('stg_google_ads__search_query_extended'), 'SUM', ['id'])
}}

),

final AS (

    SELECT
        
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'ad_group_id', 'keyword_id', 'search_term', 'ad_network_type', 'search_term_match_type']) }} AS id,
        *
    
    FROM aggregate

)

SELECT * FROM final