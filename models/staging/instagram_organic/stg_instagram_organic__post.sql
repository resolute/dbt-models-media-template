{%- set source_account_ids = var('instagram_organic_ids') -%}

{{- unused_source_check(source_account_ids) -}}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'instagram_organic_post') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'media_id']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final