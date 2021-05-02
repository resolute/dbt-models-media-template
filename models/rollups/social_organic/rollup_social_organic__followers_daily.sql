{%- set relations_list = get_social_organic_files('followers') -%}

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
    
        {{ dbt_utils.surrogate_key(['date', 'account_id']) }} AS id,
        *
    
    FROM union_tables
)

SELECT * FROM final