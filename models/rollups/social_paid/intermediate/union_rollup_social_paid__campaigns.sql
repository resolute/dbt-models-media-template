{{ dbt_utils.union_relations(
    relations= [
        ref('prep_rollup_social_paid__campaigns_facebook_ads'),
        ref('prep_rollup_social_paid__campaigns_linkedin_ads'),
        ref('prep_rollup_social_paid__campaigns_pinterest_ads')
    ]
) }}