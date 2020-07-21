{{ dbt_utils.union_relations(
    relations=[ref('stg_linkedin_ads__creatives'), ref('linkedin_ads__conversions_daily')]
) }}