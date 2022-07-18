{%- set source_account_ids = get_account_ids('linkedin ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

{%- if get_account_conversion_data_config('linkedin ads') -%}

{# Get a list of the columns from the conversion data model #}
{%- set cols = adapter.get_columns_in_relation(ref('linkedin_ads__conversions_pivoted')) -%}

{#- Create list variable of all conversion columns from the conversion data model -#}
{%- set conv_cols = [] -%}
{%- for col in cols if "conv_" in col.column -%}

    {%- do conv_cols.append(col.column) -%}

{%- endfor  -%}

WITH

ad_data AS (

    SELECT * FROM {{ ref('linkedin_ads__ads_aggregated') }}

),

conversion_data AS (

    SELECT * FROM {{ ref('linkedin_ads__conversions_pivoted') }}

),

final AS (

    SELECT

        ad_data.*
        {%- for conv_col in conv_cols -%}

        ,
        conversion_data.{{ conv_col }}

        {%- endfor %}

    FROM ad_data

    LEFT JOIN conversion_data USING(id)

)

SELECT * FROM final

{% else %}
SELECT * FROM {{ ref('linkedin_ads__ads_aggregated') }}
{% endif %}