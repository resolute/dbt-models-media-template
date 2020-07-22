{{ dbt_utils.union_relations(
    relations=[ref('stg_google_campaign_manager__ads_placement_sites'), ref('google_campaign_manager__conversions_daily')]
) }}