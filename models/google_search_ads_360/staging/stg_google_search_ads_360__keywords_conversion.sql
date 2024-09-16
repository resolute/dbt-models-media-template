{%- set source_account_ids = get_account_ids('google search ads 360') -%}

{{ config(enabled= source_account_ids|length > 0 is true and get_account_conversion_data_config('google search ads 360')) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'dcs_keywords_conversion_tag') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        account_id,
        account_name,
        date,
        agency_id,
        agency AS agency_name,
        advertiser_id,
        advertiser_name,
        engine_account_id,
        engine_account_engine_id,
        engine_account AS engine_account_name,
        engine_account_type,
        campaign_id,
        campaign_name,
        campaign_status,
        ad_group_id,
        ad_group AS ad_group_name,
        ad_group_status,
        ad_id,
        ad_name,
        keyword_id,
        keyword_text,
        keyword_labels,
        keyword_match_type,
        keyword_status, 
        keyword_click_server_url,
        device_segment,
        floodlight_group_id,
        floodlight_group,
        floodlight_activity_id, 
        floodlight_activity,

        {#- Conversions -#}
        dfa_actions AS actions,
        dfa_weighted_actions AS weighted_actions,
        dfa_transactions AS transactions,
        dfa_revenue AS revenue,
        quality_score_current

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'agency_id', 'advertiser_id', 'engine_account_id', 'campaign_id', 'ad_group_id', 'ad_id', 'keyword_id', 'device_segment', 'floodlight_group', 'floodlight_activity']) }} AS id,
        'Search Ads 360' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)