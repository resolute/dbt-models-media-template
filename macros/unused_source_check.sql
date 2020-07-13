{%- macro unused_source_check(account_ids) -%}

{%- if account_ids == [] -%}
    {{-
        config(
            schema = 'unused_models'
        )
    -}}
{%- endif -%}

{%- endmacro  -%}