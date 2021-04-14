{{ config(enabled= (var('google_campaign_manager_ids'))|length > 0 is true) }}

WITH

aggregate AS (

{{
    aggregate_model(ref('stg_google_campaign_manager__ads_placement_sites'), 'SUM', ['id', 'platform_type', 'browser_platform', 'connection_type'], [])
}}

),

final AS (

    SELECT
        
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'advertiser_id', 'site_id', 'placement_id', 'placement_size', 'ad_id', 'creative_id']) }} AS id,
        *
    
    FROM aggregate

)

SELECT * FROM final
