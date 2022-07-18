{%- set source_account_ids = get_account_ids('linkedin organic') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'linkedin_organic_share_lifetime') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'share']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final