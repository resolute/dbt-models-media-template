{{ config(enabled= (var('google_ads_ids'))|length > 0 is true) }}

{{ replace_null_values(ref('google_ads__keywords_conversions_joined')) }}