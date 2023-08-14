{%- set source_account_ids = get_account_ids('google search ads 360') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

aggregate AS (

{{
    media_template.aggregate_model(ref('stg_google_search_ads_360__keywords'), 'SUM', ['id', 'avg_pos', 'sum_pos'])
}}

),

final AS (

    SELECT
        
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'advertiser_id', 'engine_account_id', 'campaign_id', 'ad_group_id', 'keyword_id']) }} AS id,
        *
    
    FROM aggregate

)

SELECT * FROM final