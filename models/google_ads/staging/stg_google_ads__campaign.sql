{%- set source_account_ids = get_account_ids('google ads') -%}

{# Identify whether to enable this type of Google Ads model #}
{%- set enable_google_ads_model = true -%}
{%- set ev_enable_models = fromyaml(env_var('DBT_GOOGLE_ADS_MODELS_ENABLED', '')) -%}
{%- if ev_enable_models is not none and 'campaign' not in ev_enable_models -%}
    {%- set enable_google_ads_model = false -%}
{%- elif var('google_ads_models_enabled', [])|length > 0 is true and 'campaign' not in var('google_ads_models_enabled', []) -%}
    {%- set enable_google_ads_model = false -%}
{%- endif -%}

{{ config(enabled= source_account_ids|length > 0 is true and enable_google_ads_model is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_ads_campaign') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT
        
        {# Dimensions -#}
        account_id,
        account_name,
        date,
        end_date AS campaign_end_date,
        start_date AS campaign_start_date,
        customer_id,
        device,
        client_manager_id,        
        campaign_id,
        campaign_name,
        campaign_label_ids,
        campaign_labels,
        adv_channel_type AS campaign_type, {#- campaign_type denotes if campaign is Performance Max type -#}
        adv_channel_sub_type AS campaign_sub_type,        
        budget_explicitly_shared AS campaign_budget_explicitly_shared,
        budget_total_amount AS campaign_budget_total_amount,
        budget_amount AS campaign_budget_amount,
        target_roas AS campaign_target_roas,
        bid_strategy_name AS campaign_bid_strategy_name ,
        bid_strategy_type AS campaign_bid_strategy_type,
        bid_strategy_id AS campaign_bid_strategy_id,
        url_custom_parameters AS campaign_url_custom_parameters,
        final_url_suffix AS campaign_final_url_suffix,
        tracking_url_template AS campaign_tracking_url_template,
        currency_code,

        {#- General metrics -#}
        impressions,
        top_impressions,
        absolute_top_impressions,
        content_eligible_impressions,
        search_eligible_impressions,
        search_exact_match_eligible_impressions,
        cost,
        clicks AS link_clicks,
        engagements,
        interactions,

        {#- Video metrics -#}
        video_views,
        video_quartile_p25 AS video_p25_watched,
        video_quartile_p50 AS video_p50_watched,
        video_quartile_p75 AS video_p75_watched,
        video_quartile_p100 AS video_completions

        -- Excluded fields --
        /*
        url_custom_parametrs,
        all_conversions,
        all_conversions_value,
        conversions,
        conversions_value,
        view_through_conversions
        */

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'campaign_id', 'device']) }} AS id,
        'Google Ads' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final