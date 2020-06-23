WITH

data AS (
  
    SELECT * FROM {{ ref('stg_google_campaign_manager__ads_creatives_placements') }}

),

final AS (

    SELECT

        *,
        LOWER(REPLACE(activity_group, ' ', '_')) AS activity_group_formatted,
        LOWER(REPLACE(activity, ' ', '_')) AS activity_formatted

    FROM data

)

SELECT * FROM final