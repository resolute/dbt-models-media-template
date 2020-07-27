{%- macro aggregate_and_replace_nulls(relation, exclude='', aggregate_function='SUM', numeric=0.0, integer=0, float=0.0, string='null') -%}

{#- Get a list of the columns from the upstream model -#}
{%- set cols = adapter.get_columns_in_relation(relation) -%}

{%- set group_by_clause = [] -%}

SELECT
{% for col in cols -%}

{% if col.dtype == 'NUMERIC' %}
    COALESCE({{ aggregate_function }}({{ col.column }}), {{ numeric }}) AS {{ col.column }}
{%- elif col.dtype == 'INT64' %}
    COALESCE({{ aggregate_function }}({{ col.column }}), {{ integer }}) AS {{ col.column }}
{%- elif col.dtype == 'FLOAT64' %}
    COALESCE({{ aggregate_function }}({{ col.column }}), {{ float }}) AS {{ col.column }}
{%- elif col.dtype == 'STRING' %}
    COALESCE({{ col.column }}, {{ string }}) AS {{ col.column }}
    {%- do group_by_clause.append(col.column) -%}
{%- else %}
    COALESCE({{ col.column }}, null) AS {{ col.column }}
    {%- do group_by_clause.append(col.column) -%}
{%- endif %}{% if not loop.last %},{% endif %}

{%- endfor  %}

FROM {{ relation }}

{%- endmacro -%}