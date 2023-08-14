{%- set source_account_ids = get_account_ids('google search ads 360') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'dcs_ads_activities') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})
      AND floodlight_group IS NOT NULL
      AND floodlight_group != "None"

),

rename_recast AS (

    SELECT
        
        {# Dimensions -#}
        account_id,
        account_name,
        date,
        advertiser_id,
        advertiser_name,
        engine_account AS engine_account_name,
        engine AS engine_account_type,
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group AS ad_group_name,
        ad_id,
        ad_name,
        ad_labels,
        floodlight_group,
        floodlight_activity,

        {#- Conversions -#}
        actions,
        weighted_actions,
        transactions,
        dfaRevenue AS revenue

        -- Excluded fields --
        /*
        date_yyyymmdd,
        __insert_date
        */

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'advertiser_id', 'engine_account_name', 'campaign_id', 'ad_group_id', 'ad_id', 'floodlight_group', 'floodlight_activity']) }} AS id,
        'Search Ads 360' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final