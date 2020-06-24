{{ dbt_utils.union_relations(
    relations=[ref('google_campaign_manager__ads_creatives_placements_daily'), ref('google_campaign_manager__conversions_daily')]
) }}