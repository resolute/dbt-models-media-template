{{ config(enabled= (var('google_campaign_manager_ids'))|length > 0 is true) }}

{{ replace_null_values(ref('google_campaign_manager__ads_conversions_joined')) }}