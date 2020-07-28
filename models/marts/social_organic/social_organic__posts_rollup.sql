WITH

union_tables AS (

{{ dbt_utils.union_relations(
    relations= [ref('facebook_organic__posts_lifetime'), ref('instagram_organic__posts_lifetime'), ref('twitter_organic__posts_lifetime'), ref('linkedin_organic__posts_lifetime')],
    exclude= ['id']
) }}

),

final AS (

    SELECT
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'post_id']) }} AS id,
        *
    
    FROM union_tables
)

SELECT * FROM final