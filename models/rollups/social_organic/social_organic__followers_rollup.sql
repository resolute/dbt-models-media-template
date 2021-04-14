WITH

union_tables AS (

{{ dbt_utils.union_relations(
    relations= [ref('facebook_organic__followers_daily'), ref('instagram_organic__followers_daily'), ref('twitter_organic__followers_daily'), ref('linkedin_organic__followers_daily')],
    exclude= ['id']
) }}

),

final AS (

    SELECT
    
        {{ dbt_utils.surrogate_key(['date', 'account_id']) }} AS id,
        *
    
    FROM union_tables
)

SELECT * FROM final