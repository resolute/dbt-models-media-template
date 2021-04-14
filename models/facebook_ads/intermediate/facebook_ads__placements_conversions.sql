{{ config(enabled= (var('facebook_ads_ids'))|length > 0 is true) }}

{# Get a list of the columns from the upstream model #}
{%- set cols = adapter.get_columns_in_relation(ref('stg_facebook_ads__placements')) -%}

{#- Create list variable of all Facebook standard/custom conversion columns from the upstream model -#}
{%- set conv_cols = [] -%}
{%- for col in cols if "conv_fb_" in col.column -%}

    {%- do conv_cols.append(col.column) -%}

{%- endfor  -%}

WITH

final AS (

    SELECT

        {% for conv_col in conv_cols -%}

            SUM({{ conv_col }}) AS {{ conv_col }}{% if not loop.last %},{% endif %}

        {% endfor -%}
    
    FROM {{ ref('stg_facebook_ads__placements') }}

)

SELECT * FROM final