{%- set source_account_ids = get_account_ids('google search ads 360') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

aggregate AS (

{{
    media_template.aggregate_model(ref('stg_google_search_ads_360__ads'), 'SUM', ['id', 'avg_pos'])
}}

),

final AS (

    SELECT
        
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'advertiser_id', 'engine_account_name', 'campaign_id', 'ad_group_id', 'ad_id']) }} AS id,
        *
    
    FROM aggregate

)

SELECT * FROM final