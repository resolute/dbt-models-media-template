{%- set source_account_ids = var('google_campaign_manager_ids') -%}

{{- unused_source_check(source_account_ids) -}}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'dcm_geo_extended') }}

    WHERE advertiser_id IN UNNEST({{ source_account_ids }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'advertiser_id', 'site_id', 'placement_id', 'creative_id', 'activity_id', 'country', 'state', 'city', 'dma_region']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final