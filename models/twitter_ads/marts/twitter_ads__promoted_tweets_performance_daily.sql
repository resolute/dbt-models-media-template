{{ config(enabled= (var('twitter_ads_ids'))|length > 0 is true) }}

{{ replace_null_values(ref('twitter_ads__promoted_tweets_conversions_joined')) }}