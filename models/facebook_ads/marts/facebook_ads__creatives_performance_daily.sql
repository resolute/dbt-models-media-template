{{ config(enabled= (var('facebook_ads_ids'))|length > 0 is true) }}

{{ replace_null_values(ref('facebook_ads__creatives_conversions_joined')) }}