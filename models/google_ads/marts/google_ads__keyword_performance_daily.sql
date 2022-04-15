{{ config(enabled= get_account_ids('google ads')|length > 0 is true) }}

{{ replace_null_values(ref('google_ads__keywords_conversions_joined')) }}