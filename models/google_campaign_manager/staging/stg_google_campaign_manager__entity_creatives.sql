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
        account_id,
        account_name,
        date,
        advertiser_id,
        advertiser_name,
        campaign_id,
        campaign_name,
        site_id,
        site,
        placement_id,
        placement,
        ad_id,
        ad,
        creative_id,
        creative,
        ad_type,
        creative_pixel_size,
        placement_size,
        activity_group_id,
        activity_group,
        activity_id,
        activity

    FROM source_data

    QUALIFY ROW_NUMBER() OVER (PARTITION BY creative_id ORDER BY __insert_date DESC) = 1
    
)

SELECT * FROM final