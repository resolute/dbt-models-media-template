{%- set source_account_ids = get_account_ids('bing ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

aggregate AS (

{{
    aggregate_model(ref('stg_bing_ads__ads_with_description_goal'), 'SUM', ['id', 'goal', 'goal_type', 'assists_conversions', 'conversions', 'value_conversions'])
}}

),

final AS (

    SELECT
        
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'campaign_id', 'adset_id', 'ad_id']) }} AS id,
        *
    
    FROM aggregate

)

SELECT * FROM final