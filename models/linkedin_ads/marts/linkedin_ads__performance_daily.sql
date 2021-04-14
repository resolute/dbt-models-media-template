{{ config(enabled= (var('linkedin_ads_ids'))|length > 0 is true) }}

{{ replace_null_values(ref('linkedin_ads__ads_conversions_joined')) }}