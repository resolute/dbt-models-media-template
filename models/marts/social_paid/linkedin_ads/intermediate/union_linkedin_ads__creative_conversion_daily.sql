{{ dbt_utils.union_relations(
    relations=[ref('linkedin_ads__creatives_daily'), ref('linkedin_ads__conversions_daily')]
) }}