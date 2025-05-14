{%- set source_account_ids = get_account_ids('bing ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true and get_account_conversion_data_config('bing ads')) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'bing_ads_with_description_goal') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT
        {# -- Dimensions -- #}
        account_id,
        account_name,
        account_number,
        account_status,
        date,
        campaign_id,
        campaign_name,
        campaign_status,
        adset_id,
        adset_name,
        adset_status,
        ad_id,
        ad_title AS ad_name,
        ad_description,
        title_part_1,
        title_part_2,
        goal,
        goal_type,
        display_url,

        {# -- General Metrics -- #}
        imps AS impressions,
        spend AS cost,
        clicks AS link_clicks,
        conv AS conversions,
        revenue AS value_conversions,
        assists AS assists_conversions
        
    

        --Excluded fields
        /*
        __insert_date,
        date_yyyymmdd,
        avg_pos,
        */

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'ad_id', 'adset_id', 'goal']) }} AS id,
        'Bing Ads' AS data_source,
        'Bing' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final