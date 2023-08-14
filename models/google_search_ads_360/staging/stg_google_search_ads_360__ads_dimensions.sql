{%- set source_account_ids = get_account_ids('google search ads 360') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_search_ads_360_ads_dimensions') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

rename_recast_aggregate AS (

    SELECT
        
        account_id,
        account_name,
        account_engine_id,
        ad_engine_id,
        account_type AS engine_account_type,
        engine_status,
        campaign_id,
        campaign_name,
        campaign_status,
        ad_group_id,
        ad_group_name,
        ad_group_status, 
        ad_name,
        ad_id,
        ad_landing_page,
        ad_status,
        ad_display_url,
        expanded_text_ad_headline AS ad_title,
        expanded_text_ad_headline1 AS ad_title_2,
        expanded_text_ad_headline2 AS ad_title_3,
        description_line1 AS line_1,
        description_line2 AS line_2,
        effective_labels,
        last_modified_timestamp

        -- Excluded fields --
        /*
        date_yyyymmdd,
        __insert_date
        */

    FROM source_data

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.generate_surrogate_key(['account_id', 'account_engine_id', 'campaign_id', 'ad_group_id', 'ad_id']) }} AS id,
        'Search Ads 360' AS data_source,
        'Google' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,        
        *
    
    FROM rename_recast_aggregate

)

SELECT * FROM final