{{ dbt_utils.union_relations(
    relations=[ref('stg_google_ads__keywords_extended')]
) }}