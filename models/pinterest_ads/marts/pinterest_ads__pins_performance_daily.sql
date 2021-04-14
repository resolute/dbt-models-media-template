{{ config(enabled= (var('pinterest_ads_ids'))|length > 0 is true) }}

{{ media_template.replace_null_values(ref('stg_pinterest_ads__pins_1v_30en_30cl')) }}