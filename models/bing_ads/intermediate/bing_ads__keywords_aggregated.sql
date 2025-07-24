{%- set source_account_ids = get_account_ids('bing ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

aggregate AS (

{{
    aggregate_model(ref('stg_bing_ads__keywords_by_goal'), 'SUM', ['id', 'goal', 'goal_type', 'all_conversions', 'conversions', 'all_conversions_qualified', 'value_conversions'])
}}

),

final AS (

    SELECT
        
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'adset_id', 'ad_distribution', 'keyword_id']) }} AS id,
        aggregate.*
    
    FROM aggregate

)

SELECT * FROM final