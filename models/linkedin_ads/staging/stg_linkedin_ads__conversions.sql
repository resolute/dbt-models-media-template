{{ config(enabled=var('linkedin_ads_conversions_enabled')) }}

{%- set source_account_ids = var('linkedin_ads_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'linkedin_ads_conversions') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        account_id,
        account_name,
        date,
        campaign_group_id,
        campaign_group_name,
        campaign_id,
        campaign_name,
        campaign_type,
        daily_budget,
        conversion_id,
        conversion_name,
        conversion_type,
        currency_code,
        last_modified,

        {#- Conversions -#}
        conversions,
        conversion_value AS value_conversions,
        post_click_conversions AS conversions_click_through,
        post_view_conversions AS conversions_view_through,
        viral_conversions,
        viral_post_click_conversions AS viral_conversions_click_through,
        viral_post_view_conversions AS viral_conversions_view_through

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_id', 'conversion_id']) }} AS id,
        'LinkedIn Paid' AS data_source,
        'LinkedIn' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Social' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final