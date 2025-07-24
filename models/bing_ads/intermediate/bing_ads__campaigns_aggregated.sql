{%- set source_account_ids = get_account_ids('bing ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

aggregate AS (

{{
    aggregate_model(ref('stg_bing_ads__campaign_goal'), 'SUM', ['id', 'goal', 'goal_type', 'assists_conversions', 'conversions', 'value_conversions', 'all_conversions_qualified', 'conversions_qualified', 'view_through_conversions', 'view_through_conversions_qualified', 'view_through_value_conversions','all_value_conversions'])
}}

),

final AS (

    SELECT
        
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'campaign_id']) }} AS id,
        *
    
    FROM aggregate

)

SELECT * FROM final
