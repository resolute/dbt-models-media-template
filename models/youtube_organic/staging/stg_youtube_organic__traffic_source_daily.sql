{%- set source_account_ids = get_account_ids('youtube organic') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'youtube_organic_traffic_source_daily') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'traffic_source', 'subscribed_status', 'live_or_on_demand']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final