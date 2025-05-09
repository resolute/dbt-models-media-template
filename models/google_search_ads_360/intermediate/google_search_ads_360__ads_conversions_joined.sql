{%- set source_account_ids = get_account_ids('google search ads 360') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

{%- if get_account_conversion_data_config('google search ads 360') is true -%}

{# Get a list of the columns from the conversion data model #}
{%- set cols = adapter.get_columns_in_relation(ref('google_search_ads_360__ads_conversions_pivoted')) -%}

{#- Create list variable of all conversion columns from the conversion data model -#}
{%- set conv_cols = [] -%}
{%- for col in cols if "conv_" in col.column -%}

    {%- do conv_cols.append(col.column) -%}

{%- endfor  -%}

WITH

ad_data AS (

    SELECT * FROM {{ ref('google_search_ads_360__ads_aggregated') }}

),

conversion_data AS (

    SELECT * FROM {{ ref('google_search_ads_360__ads_conversions_pivoted') }}

),

final AS (

    SELECT

        COALESCE(ad_data.id, conversion_data.id) AS id,
        COALESCE(ad_data.data_source, conversion_data.data_source) AS data_source,
        COALESCE(ad_data.channel_source_name, conversion_data.channel_source_name) AS channel_source_name,
        COALESCE(ad_data.channel_source_type, conversion_data.channel_source_type) AS channel_source_type,
        COALESCE(ad_data.channel_name, conversion_data.channel_name) AS channel_name,
        COALESCE(ad_data.account_id, conversion_data.account_id) AS account_id,
        COALESCE(ad_data.account_name, conversion_data.account_name) AS account_name,
        COALESCE(ad_data.date, conversion_data.date) AS date,
        COALESCE(ad_data.advertiser_id, conversion_data.advertiser_id) AS advertiser_id,
        COALESCE(ad_data.advertiser_name, conversion_data.advertiser_name) AS advertiser_name,
        COALESCE(ad_data.engine_account_name, conversion_data.engine_account_name) AS engine_account_name,
        COALESCE(ad_data.engine_account_type, conversion_data.engine_account_type) AS engine_account_type,
        COALESCE(ad_data.campaign_id, conversion_data.campaign_id) AS campaign_id,
        COALESCE(ad_data.campaign_name, conversion_data.campaign_name) AS campaign_name,
        COALESCE(ad_data.ad_group_id, conversion_data.ad_group_id) AS ad_group_id,
        COALESCE(ad_data.ad_group_name, conversion_data.ad_group_name) AS ad_group_name,
        COALESCE(ad_data.ad_id, conversion_data.ad_id) AS ad_id,
        COALESCE(ad_data.ad_name, conversion_data.ad_name) AS ad_name,
        COALESCE(ad_data.ad_labels, conversion_data.ad_labels) AS ad_labels,

        COALESCE(ad_data.impressions, 0) AS impressions,
        COALESCE(ad_data.cost, 0) AS cost,
        COALESCE(ad_data.link_clicks, 0) AS link_clicks,
        COALESCE(ad_data.visits, 0) AS visits
        
        {%- for conv_col in conv_cols -%}

        ,
        COALESCE(conversion_data.{{ conv_col }}, 0) AS {{ conv_col }}

        {%- endfor %}

    FROM ad_data

    FULL JOIN conversion_data USING(id)

)

SELECT * FROM final

{% else %}
SELECT * FROM {{ ref('google_search_ads_360__ads_aggregated') }}
{% endif %}