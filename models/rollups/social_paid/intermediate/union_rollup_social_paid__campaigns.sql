{%- set relations_list = get_social_paid_files() -%}

{%- if relations_list|length < 1 -%}
{{ config(enabled=false) }}
{%- endif -%}

{{ dbt_utils.union_relations(
    relations= relations_list
) }}