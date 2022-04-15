{{ config(enabled= get_account_ids('facebook ads')|length > 0 is true) }}

{{ replace_null_values(ref('facebook_ads__creatives_conversions_joined')) }}