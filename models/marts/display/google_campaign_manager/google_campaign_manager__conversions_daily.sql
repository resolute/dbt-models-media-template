{%- set conversion_fields = ["total_conversions", "view_through_conversions", "click_through_conversions", "total_conversions_revenue", "view_through_revenue", "click_through_revenue"] -%}

WITH

data AS (
  
    SELECT * FROM {{ ref('google_campaign_manager__conversions_setup') }}

),

final AS (

    SELECT

        {{ dbt_utils.surrogate_key(['date', 'account_id', 'advertiser_id', 'campaign_id', 'site_id', 'placement_id', 'ad_id', 'creative_id']) }} AS id,
        account_id,
        date,
        advertiser_id,
        campaign_id,
        site_id,
        placement_id,
        ad_id,
        creative_id,

        {%- for conversion_field in conversion_fields -%}

            {{- dbt_utils.pivot(
                'activity_group_formatted',
                dbt_utils.get_column_values(ref('google_campaign_manager__conversions_setup'), 'activity_group_formatted'),
                True,
                'sum',
                '=',
                conversion_field ~ '_activity_group_',
                '',
                conversion_field,
                'NULL',
                True
            ) -}}{%- if not loop.last -%},{%- endif -%}

        {%- endfor -%},

        {%- for conversion_field in conversion_fields -%}

            {{- dbt_utils.pivot(
                'activity_formatted',
                dbt_utils.get_column_values(ref('google_campaign_manager__conversions_setup'), 'activity_formatted'),
                True,
                'sum',
                '=',
                conversion_field ~ '_activity_',
                '',
                conversion_field,
                'NULL',
                True
            ) -}}{%- if not loop.last -%},{%- endif -%}

        {%- endfor -%}

    FROM data

    GROUP BY 1,2,3,4,5,6,7,8,9

)

SELECT * FROM final