{#- Get a list of fields that are not conversions -#}
{%- set cols = adapter.get_columns_in_relation(ref('facebook_ads__performance_daily')) -%}
{%- set non_conversions = [] -%}
{%- for col in cols -%}
    {%- if "conv_" not in col.column -%}
        {%- do non_conversions.append(col.column) -%}
    {%- endif -%}
{%- endfor -%}

WITH

unpivot AS (

    {{ dbt_utils.unpivot(
        relation=ref('facebook_ads__performance_daily'),
        cast_to='string',
        exclude=non_conversions,
        remove=[],
        field_name="conversion_name",
        value_name="conversion_value"
    ) }}

),

final AS (

    SELECT DISTINCT

        account_id,
        account_name,
        campaign_id,
        campaign_name,
        --adset_id,
        --adset_name,
        --ad_id,
        --ad_name,
        --creative_id,
        --creative_name,
        conversion_name,
        REGEXP_EXTRACT(conversion_name, r"^conv_fb_(.+)_[0-9]{1,2}c_[0-9]{1,2}v$") AS conversion_name_extracted

    FROM unpivot

)

SELECT * FROM final