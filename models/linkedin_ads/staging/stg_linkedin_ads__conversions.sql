{{ config(enabled=var('linkedin_ads_conversions_enabled')) }}

{%- set source_account_ids = var('linkedin_ads_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'linkedin_ads_creatives_conversions') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        account_id,
        account_name,
        date,
        campaign_id,
        campaign_name,
        campaign_type,
        creative_id,
        ad_creative_name AS creative_name,
        ad_creative_text AS creative_text,
        ad_creative_title AS creative_title,
        creative_status,
        destination_url,
        conversion_id,
        conversion_name,
        conversion_type,

        {#- Conversions -#}
        external_website_conversions AS conversions,
        post_click_conv AS conversions_click_through,
        post_view_conv AS conversions_view_through,
        viral_conversions,
        viral_post_click_conversions AS viral_conversions_click_through,
        viral_post_view_conversions AS viral_conversions_view_through

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'creative_id', 'conversion_id']) }} AS id,
        'LinkedIn Paid' AS data_source,
        'LinkedIn' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Social' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final