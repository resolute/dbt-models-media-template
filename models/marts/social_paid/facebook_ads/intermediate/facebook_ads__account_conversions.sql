{# Get a list of the columns from the upstream model #}
{%- set cols = adapter.get_columns_in_relation(ref('stg_facebook_ads__creative')) -%}

{#- Create list variable of all non Facebook custom conversion columns from the upstream model -#}
{%- set non_custom_conv_cols = [] -%}
{%- for col in cols if "dynamic_" not in col.column -%}

    {%- do non_custom_conv_cols.append(col.column) -%}

{%- endfor  -%}

WITH

-- Unpivot all Facebook custom conversion columns to rows
unpivot AS (

{{ dbt_utils.unpivot(ref('stg_facebook_ads__creative'), cast_to='float64', exclude=[], remove=non_custom_conv_cols) }}

),

-- Aggreate the Facebook custom conversion values grouped by each custom conversion name
aggregate AS (

    SELECT

        field_name,

        SUM(value) AS value

    FROM unpivot

    GROUP BY 1

),

-- Keep only the Facebook custom conversions that have values that are not zero
final AS (

    SELECT

        field_name
    
    FROM aggregate

    WHERE value != 0

)

SELECT * FROM final