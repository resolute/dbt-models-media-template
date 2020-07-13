{%- macro unused_model_check(account_ids) -%}

{%- for account_id in account_ids if account_id|length > 0 -%}
    {{ return(true) }}
{%- else -%}
    {{ return(false) }}
{%- endfor -%}

{%- endmacro  -%}