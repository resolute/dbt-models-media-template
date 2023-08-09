{%- set source_account_ids = get_account_ids('amazon ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

{{ dbt_utils.union_relations(
    relations= [ref('amazon_ads__sponsored_brand_videos_joined'), ref('amazon_ads__sponsored_brands_joined'), ref('amazon_ads__sponsored_products_joined'), ref('amazon_ads__sponsored_display_joined'),]
) }}