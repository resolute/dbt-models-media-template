{%- set source_account_ids = get_account_ids('bing ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'bing_ads_microsoft_advertising_campaign_goal') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast AS (

    SELECT
        {# -- Dimensions -- #}
        account_id,
        account_name,
        date,
        campaign_id,
        campaign_name,
        campaign_type,
        campaign_labels,
        goal,
        goal_type,

        {# -- General Metrics -- #}
        imps AS impressions,
        spend AS cost,
        clicks AS link_clicks,       
        conv AS conversions,
        revenue AS value_conversions,
        assists AS assists_conversions,
        conversions_qualified,
        view_through_conversions, 
        view_through_conversions_qualified, 
        view_through_revenue AS view_through_value_conversions,
        all_conversions_qualified,
        all_revenue AS all_value_conversions,
        video_views,
        completed_video_views AS video_completions
    

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
    
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'campaign_id', 'goal']) }} AS id,
        'Bing Ads' AS data_source,
        'Bing' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final