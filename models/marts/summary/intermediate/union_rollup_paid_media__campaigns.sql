{{ dbt_utils.union_relations(
    relations= [ref('prep_rollup_paid_media__campaigns_social_paid'), ref('prep_rollup_paid_media__campaigns_display'), ref('prep_rollup_paid_media__campaigns_search_paid')]
) }}