{%- set relations_list = get_search_paid_files() -%}

{%- if relations_list|length < 1 -%}
{{ config(enabled=false) }}
{%- endif -%}

{{ replace_null_values(ref('union_rollup_search_paid__campaigns')) }}