WITH

final AS (

    SELECT

        {{ dbt_utils.surrogate_key(['data_source', 'account_id','channel_source_name', 'channel_source_type', 'channel_name', 'date']) }} AS id,
        *

    FROM `yale-opac.marts.executive_summary`

)

SELECT * FROM final