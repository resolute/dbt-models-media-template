{%- set source_account_ids = var('google_campaign_manager_ids') -%}

{{- unused_source_check(source_account_ids) -}}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'dcm_ads_placement_sites') }}

    WHERE advertiser_id IN UNNEST({{ source_account_ids }})

),

recast AS (

    SELECT *

        REPLACE(CAST(campaign_start_date AS DATE) AS campaign_start_date)

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'advertiser_id', 'campaign_id', 'site_id_dcm', 'placement_id', 'ad_id', 'creative_id']) }} AS id,
        *
    
    FROM recast

)

SELECT * FROM final