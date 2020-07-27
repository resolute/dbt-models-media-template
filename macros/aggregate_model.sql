{%- macro aggregate_model(relation, aggregate_function='SUM', exclude_cols=[], exclude_agg_cols=[]) -%}

{#- Get a list of the columns from the upstream model -#}
{%- set cols = adapter.get_columns_in_relation(relation) -%}

{%- set group_by_clause = [] -%}

{% for col in cols -%}

    {#- If an exclude_cols list was provided and the column is in the list, do nothing -#}
    {%- if exclude_cols and col.column in exclude_cols -%}
        {%- do cols.remove(col) -%}
    {%- endif %}

{%- endfor  %}

SELECT
{% for col in cols -%}

    {#- If an exclude_agg_cols list was provided and the column is in the list, then do not aggreagte the column -#}
    {%- if exclude_agg_cols and col.column in exclude_agg_cols %}
    {{ col.column }}
    {%- do group_by_clause.append(col.column) -%}

    {#- If the column data type is numeric, integer, or float, then aggregate -#}
    {%- elif col.dtype == 'NUMERIC' or col.dtype == 'INT64' or col.dtype == 'FLOAT64' %}
    {{ aggregate_function }}({{ col.column }}) AS {{ col.column }}

    {#- Else do not aggreagte the column -#}
    {%- else %}
    {{ col.column }}
    {%- do group_by_clause.append(col.column) -%}
    {%- endif %}{% if not loop.last %},{% endif %}

{%- endfor  %}

FROM {{ relation }}

GROUP BY {{ group_by_clause|join(', ') }}

{%- endmacro -%}