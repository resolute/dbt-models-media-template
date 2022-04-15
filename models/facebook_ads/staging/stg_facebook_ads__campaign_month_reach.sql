{%- set source_account_ids = get_account_ids('facebook ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'facebook_campaign_month_reach') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

facebook_entity_campaign_data AS (

    SELECT * FROM {{ ref('stg_facebook_ads__entity_campaigns') }}

),

facebook_entity_account_data AS (

    SELECT * FROM {{ ref('stg_facebook_ads__entity_accounts') }}

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        source_data.account_id,
        facebook_entity_account_data.account_name AS account_name,
        source_data.account_name AS account_name_on_date,
        source_data.date AS month_start_date,
        source_data.end_date AS month_end_date,
        source_data.campaign_id,
        facebook_entity_campaign_data.campaign_name AS campaign_name,
        source_data.campaign_name AS campaign_name_on_date,

        {#- General metrics -#}
        reach,
        estimated_ad_recallers,
        unique_clicks,
        unique_outbound_clicks_value,
        unique_inline_link_clicks,
        unique_like_value,
        unique_video_view_value,
        unique_landing_page_view_value,
        unique_offsite_conversion_value,
        unique_fb_pixel_add_to_cart_value,
        unique_mobile_app_install_value

        -- Excluded fields --
        /*
        frequency,
        */


    FROM source_data

    LEFT JOIN facebook_entity_campaign_data
        ON source_data.campaign_id = facebook_entity_campaign_data.campaign_id

    LEFT JOIN facebook_entity_account_data
        ON source_data.account_id = facebook_entity_account_data.account_id

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['month_start_date', 'account_id', 'campaign_id']) }} AS id,
        'Facebook Paid' AS data_source,
        'Facebook' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Social' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final