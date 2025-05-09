{%- set source_account_ids = get_account_ids('google search ads 360') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

{{ media_template.replace_null_values(ref('google_search_ads_360__keywords_conversions_joined')) }}