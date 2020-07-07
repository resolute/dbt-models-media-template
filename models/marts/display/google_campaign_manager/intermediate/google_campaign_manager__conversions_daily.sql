{%- set conversion_fields = ["total_conversions", "view_through_conversions", "click_through_conversions", "total_conversions_revenue", "view_through_revenue", "click_through_revenue"] -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_google_campaign_manager__ads_creatives_placements') }}

),

general_definitions AS (

    SELECT
    
        *,
        'Campaign Manager' AS data_source,
        'Campaign Manager' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Display' AS channel_name
  
    FROM data
    
),

pivot_conversions AS (

    SELECT

        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        date,
        advertiser_id,
        advertiser_name,
        campaign_id,
        campaign_name,
        site_id,
        site,
        placement_id,
        placement,
        ad_id,
        ad,
        creative_id,
        creative,
        ad_type,
        creative_pixel_size,
        placement_size,

        {%- for conversion_field in conversion_fields -%}

            {{- dbt_utils.pivot(
                'activity_group',
                dbt_utils.get_column_values(ref('stg_google_campaign_manager__ads_creatives_placements'), 'activity_group')|map('replace',' ','_')|list,
                True,
                'sum',
                '=',
                'conv_gcm_' ~ conversion_field ~ '_activity_group_',
                '',
                conversion_field,
                'NULL',
                True
            ) -}}{%- if not loop.last -%},{%- endif -%}

        {%- endfor -%},

        {%- for conversion_field in conversion_fields -%}

            {{- dbt_utils.pivot(
                'activity',
                dbt_utils.get_column_values(ref('stg_google_campaign_manager__ads_creatives_placements'), 'activity')|map('replace',' ','_')|list,
                True,
                'sum',
                '=',
                'conv_gcm_' ~ conversion_field ~ '_activity_',
                '',
                conversion_field,
                'NULL',
                True
            ) -}}{%- if not loop.last -%},{%- endif -%}

        {%- endfor -%}

    FROM general_definitions

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22

),

final AS (

    SELECT

        {{ dbt_utils.surrogate_key(['date', 'account_id', 'advertiser_id', 'campaign_id', 'site_id', 'placement_id', 'ad_id', 'creative_id']) }} AS id,
        *

    FROM pivot_conversions

)

SELECT * FROM final