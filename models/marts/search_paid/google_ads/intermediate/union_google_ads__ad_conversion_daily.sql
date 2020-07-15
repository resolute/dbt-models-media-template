{{ dbt_utils.union_relations(
    relations=[ref('stg_google_ads__ads'), ref('google_ads__ad_conversions_daily')]
) }}