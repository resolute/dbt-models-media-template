{{ config(enabled= (var('linkedin_ads_ids'))|length > 0 is true) }}

WITH

aggregate AS (

{{
    aggregate_model(ref('stg_linkedin_ads__creatives'), 'SUM', ['id', 'creative_id', 'creative_name', 'creative_text', 'creative_title', 'creative_status', 'destination_url', 'video_duration'], [])
}}

),

final AS (

    SELECT
        
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_id']) }} AS id,
        *
    
    FROM aggregate

)

SELECT * FROM final
