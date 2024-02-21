{%- set source_account_ids = get_account_ids('google search ads 360') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

ad_data AS (

    SELECT * FROM {{ ref('google_search_ads_360__ads_conversions_joined') }}

),


ad_copy AS (

    SELECT * FROM {{ ref('stg_google_search_ads_360__ads_dimensions') }}

),

join_ad_and_ad_copy AS (

    SELECT

        ad_data.*,
        COALESCE(ad_copy.ad_status, '') AS ad_status,
        COALESCE(ad_copy.effective_labels, '') AS effective_labels,
        COALESCE(ad_copy.ad_title, '') AS ad_title,
        COALESCE(ad_copy.ad_title_2, '') AS ad_title_2,
        COALESCE(ad_copy.ad_display_url, '') AS ad_display_url,
        COALESCE(ad_copy.line_1, '') AS line_1,
        COALESCE(ad_copy.line_2, '') AS line_2,

    FROM ad_data

    LEFT JOIN ad_copy 
        ON(ad_data.engine_account_type = ad_copy.engine_account_type
        AND ad_data.campaign_id = ad_copy.campaign_id
        AND ad_data.ad_group_id = ad_copy.ad_group_id
        AND ad_data.ad_id = ad_copy.ad_id
        )

),

final AS (

    SELECT

        *,
        CONCAT(ad_title, ' | ', ad_title_2, '\n', ad_display_url, '\n', line_1, ' ', line_2) AS ad_copy

    FROM join_ad_and_ad_copy

)

SELECT * FROM final