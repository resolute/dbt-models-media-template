{%- set source_account_ids = get_account_ids('google campaign manager') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_cm_ads_creatives_placements') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

final AS (

    SELECT
    
        {# Dimensions -#}
        date,
        creative_id,
        creative,
        creative_pixel_size

    FROM source_data

    QUALIFY ROW_NUMBER() OVER (PARTITION BY creative_id ORDER BY __insert_date DESC) = 1
    
)

SELECT * FROM final