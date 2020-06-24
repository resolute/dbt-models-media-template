WITH

data AS (
  
    SELECT * FROM {{ ref('stg_google_campaign_manager__ads_creatives_placements') }}

),

general_definitions AS (

    SELECT
    
        *,
        'Campaign Manager' AS data_source,
        'Campaign Manager' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Display' AS channel_name
  
    FROM data
    
),

final AS (

    SELECT

        *,
        LOWER(REPLACE(activity_group, ' ', '_')) AS activity_group_formatted,
        LOWER(REPLACE(activity, ' ', '_')) AS activity_formatted

    FROM general_definitions

)

SELECT * FROM final