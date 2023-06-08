{%- set relations_list = get_social_organic_files('posts') -%}

{%- if relations_list|length < 1 -%}
{{ config(enabled=false) }}
{%- endif -%}

WITH

union_tables AS (

{{ dbt_utils.union_relations(
    relations= relations_list,
    exclude= ['id']
) }}

),

final AS (

    SELECT
    
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'post_id']) }} AS id,
        *
    
    FROM union_tables
)

SELECT * FROM final