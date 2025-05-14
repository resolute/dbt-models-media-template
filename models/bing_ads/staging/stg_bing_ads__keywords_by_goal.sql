{%- set source_account_ids = get_account_ids('bing ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'bing_keywords_by_goals') }}
    
    WHERE account_id IN UNNEST({{ source_account_ids }})
    
),

rename_recast AS (

    SELECT

        {# -- Dimensions -- #}
        account_id,
        account_name,
        account_number,
        date,
        campaign_id,
        campaign_name,
        adset_id,
        adset_name,
        keyword_id,
        keyword AS keyword_name,
        bid_match_type,
        destination_url,
        current_max_cpc,
        ad_distribution,
        goal,
        goal_type,

        {# -- General Metrics -- #}
        imps AS impressions,
        spend AS cost,
        clicks AS link_clicks,
        conv AS conversions,
        revenue AS value_conversions,
        all_conversions,
        all_conversions_qualified
        

        --Excluded columns
        /*
        date_yyyymmdd,
        __insert_date
        conversion_rate,
        average_cpc,
        avg_pos, - depricated
        quality_score
        */

    FROM source_data

),

final AS (
  
    SELECT 

        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'adset_id', 'ad_distribution', 'keyword_id', 'goal']) }} AS id,
        'Bing Ads' AS data_source,
        'Bing' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Paid Search' AS channel_name,
        *
    
    FROM rename_recast

)

SELECT * FROM final