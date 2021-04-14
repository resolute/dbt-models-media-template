{{ config(enabled= (var('facebook_ads_ids'))|length > 0 is true) }}

{# Get a list of the columns from the upstream model #}
{%- set cols = adapter.get_columns_in_relation(ref('stg_facebook_ads__placements')) -%}

{#- Get a list of the Facebook Ads conversions that are active for the Facebook Ads accounts -#}
{%- set conversions = dbt_utils.get_query_results_as_dict("select * from" ~ ref('facebook_ads__placements_conversions')) -%}
{%- set active_conversions = [] -%}
{%- for column, value in conversions.items() -%}
    {%- if value[0] != 0.0 -%}
        {% do active_conversions.append(column) -%}
    {%- endif -%}
{%- endfor -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_facebook_ads__placements') }}

),

final AS (

    SELECT
    
        {# Dimensions and non-Conversion Metrics -#}

        {#- Loop through each upstream model column that is not a conversion -#}
        {%- for col in cols if "conv_" not in col.column -%}
            {{ col.column }}{% if not loop.last or active_conversions != [] %},{% endif %}
        {% endfor %}

        {#- Conversions -#}

        {#- Loop through each active conversion -#}
        {%- for conv in active_conversions -%}
            {{ conv }}{% if not loop.last %},{% endif %}
        {% endfor %}
        
    FROM data

)
  
SELECT * FROM final