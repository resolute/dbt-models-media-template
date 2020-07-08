{%- set source_account_ids = var('google_ads_ids') -%}

{{- unused_source_check(source_account_ids) -}}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'google_adwords_search_query_extended') }}

    WHERE account_id IN UNNEST({{ source_account_ids }})

),

final AS (
  
    SELECT 
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'campaign_id', 'adset_id', 'keyword_id', 'search_term', 'query_match_type']) }} AS id,
        *
    
    FROM source_data

)

SELECT * FROM final