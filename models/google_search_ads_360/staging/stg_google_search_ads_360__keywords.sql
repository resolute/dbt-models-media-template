{%- set source_account_ids = get_account_ids('google search ads 360') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'dcs_keywords_engine') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT

        {# Dimensions -#}
        account_id,
        account_name,
        date,
        advertiser_id,
        advertiser_name,
        ds_account_id AS engine_account_id,
        account_engine_id AS engine_account_engine_id,
        engine_account_name,
        engine_account_type, 
        campaign_id,
        campaign_name,
        adgroup_id AS ad_group_id,
        adgroup_name AS ad_group_name,
        keyword_id,
        keyword_text,
        keyword_labels,
        avg_pos,
        sum_pos,
        
        {#- General metrics -#}
        impr AS impressions,
        cost,
        clicks AS link_clicks,
        visits

        -- Excluded fields --
        /*
        ad_words_conversions,
        dfa_revenue,
        */

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'advertiser_id', 'engine_account_id', 'campaign_id', 'ad_group_id', 'keyword_id']) }} AS id,
        'Search Ads 360' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final