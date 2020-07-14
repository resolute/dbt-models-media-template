{%- set source_account_ids = var('google_ads_ids') -%}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_adwords_search_query_conversions') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_id', 'ad_group_id', 'keyword_id', 'search_term', 'match_type', 'network', 'conv_tracker_id']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final