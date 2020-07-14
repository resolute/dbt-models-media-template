{{ dbt_utils.union_relations(
    relations=[ref('google_ads__search_query_daily'), ref('google_ads__search_query_conversions_daily')]
) }}