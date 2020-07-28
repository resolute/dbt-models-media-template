{%- macro test() -%}

{% set upstream_models = model.depends_on.nodes|unique|list -%}
{{ upstream_models }}

{%- endmacro  -%}