{%- set source_account_ids = get_account_ids('instagram organic') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'instagram_organic_post') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'media_id']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final