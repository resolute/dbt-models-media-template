WITH

aggregate AS (

{{
    aggregate_model(ref('stg_google_ads__search_query_extended'), 'SUM', ['id'], ['avg_pos'])
}}

),

final AS (

    SELECT
        
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'ad_group_id', 'keyword_id', 'ad_network_type_1', 'search_term', 'query_match_type']) }} AS id,
        *
    
    FROM aggregate

)

SELECT * FROM final