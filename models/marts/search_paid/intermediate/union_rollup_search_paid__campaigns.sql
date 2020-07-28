{{ dbt_utils.union_relations(
    relations= [ref('prep_rollup_search_paid__campaigns_google_ads')]
) }}