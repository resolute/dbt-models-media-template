{%- macro replace_null_values(relation, numeric=0.0, integer=0, float=0.0, string='null') -%}

{#- Get a list of the columns from the upstream model -#}
{%- set cols = adapter.get_columns_in_relation(relation) -%}

SELECT
{% for col in cols -%}

{% if col.dtype == 'NUMERIC' %}
    COALESCE({{ col.column }}, {{ numeric }}) AS {{ col.column }}
{%- elif col.dtype == 'INT64' %}
    COALESCE({{ col.column }}, {{ integer }}) AS {{ col.column }}
{%- elif col.dtype == 'FLOAT64' %}
    COALESCE({{ col.column }}, {{ float }}) AS {{ col.column }}
{%- elif col.dtype == 'STRING' %}
    COALESCE({{ col.column }}, {{ string }}) AS {{ col.column }}
{%- else %}
    {{ col.column }}
{%- endif %}{% if not loop.last %},{% endif %}

{%- endfor  %}

FROM {{ relation }}

{%- endmacro -%}