{{ config(enabled= get_account_ids('google campaign manager')|length > 0 is true) }}

{{ replace_null_values(ref('google_campaign_manager__ads_conversions_joined')) }}