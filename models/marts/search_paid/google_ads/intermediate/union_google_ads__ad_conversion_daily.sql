{{ dbt_utils.union_relations(
    relations=[ref('google_ads__ads_daily'), ref('google_ads__ad_conversions_daily')]
) }}