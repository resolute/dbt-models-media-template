{{ dbt_utils.union_relations(
    relations= [ref('prep_rollup_display__campaigns_google_campaign_manager')]
) }}