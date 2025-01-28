{%- set source_account_ids = get_account_ids('google campaign manager') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_cm_ads_creatives_placements') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT
    
        {# Dimensions -#}
        date,
        creative_id,
        creative,
        creative_pixel_size

    FROM source_data

    QUALIFY ROW_NUMBER() OVER (PARTITION BY creative_id ORDER BY __insert_date DESC) = 1
    
),

final AS (

    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['date', 'creative_id', 'creative_pixel_size']) }} AS id,
        'Campaign Manager' AS data_source,
        'Campaign Manager' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Display' AS channel_name,
        *
    
    FROM rename_recast

)


SELECT * FROM final