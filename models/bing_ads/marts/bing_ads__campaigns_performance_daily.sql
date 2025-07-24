{{ config(enabled= get_account_ids('bing ads')|length > 0 is true) }}

{{ replace_null_values(ref('bing_ads__campaigns_goals_joined')) }}