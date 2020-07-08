{%- set source_account_ids = var('pinterest_ads_ids') -%}

{{- unused_source_check(source_account_ids) -}}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'pinterest_ads_audience_groups') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'scope', 'type', 'dimension_value']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final