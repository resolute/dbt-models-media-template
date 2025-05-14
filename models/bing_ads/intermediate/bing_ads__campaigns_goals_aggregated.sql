{%- set source_account_ids = get_account_ids('bing ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

aggregate AS (

{{
    aggregate_model(ref('bing_ads__campaigns_goals_pivoted'), 'SUM')
}}

),

final AS (

    SELECT
        
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'campaign_id']) }} AS id,
        *
    
    FROM aggregate

)

SELECT * FROM final
