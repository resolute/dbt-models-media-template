{%- set source_account_ids = get_account_ids('amazon ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

{{ replace_null_values(ref('amazon_ads__campaign_type_union')) }}