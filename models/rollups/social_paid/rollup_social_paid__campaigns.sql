{%- set relations_list = get_social_paid_files(return_files=false) -%}

{%- if relations_list|length < 1 -%}
{{ config(enabled=false) }}
{%- endif -%}

{{ replace_null_values(ref('union_rollup_social_paid__campaigns')) }}