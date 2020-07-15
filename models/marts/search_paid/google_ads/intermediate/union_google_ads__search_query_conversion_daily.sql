{{ dbt_utils.union_relations(
    relations=[ref('stg_google_ads__search_query_extended'), ref('google_ads__search_query_conversions_daily')]
) }}