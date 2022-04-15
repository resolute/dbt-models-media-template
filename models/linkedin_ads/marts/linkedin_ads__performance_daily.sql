{{ config(enabled= get_account_ids('linkedin ads')|length > 0 is true) }}

{{ replace_null_values(ref('linkedin_ads__ads_conversions_joined')) }}