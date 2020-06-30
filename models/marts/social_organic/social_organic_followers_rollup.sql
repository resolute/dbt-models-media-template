{# Create dictionary variable of upstream models that could be used in the rollup #}
{%- 
set upstream_models = {
    'facebook': {
        'has_data': var('facebook_organic_ids') != None,
        'model_ref': 'facebook_organic__followers_daily'
    },
    'instagram': {
        'has_data': var('instagram_organic_ids') != None,
        'model_ref': 'instagram_organic__followers_daily'
    },
    'twitter': {
        'has_data': var('twitter_organic_ids') != None,
        'model_ref': 'twitter_organic__followers_daily'
    },
    'linkedin': {
        'has_data': var('linkedin_organic_ids') != None,
        'model_ref': 'linkedin_organic__followers_daily'
    }
}
-%}

{# Enable this model only if any of the upstream models have data #}
{{-
    config(
        enabled = (upstream_models["facebook"].has_data or upstream_models["instagram"].has_data or upstream_models["twitter"].has_data or upstream_models["linkedin"].has_data) == true
    )
-}}


WITH

union_tables AS (
    
    {# Loop through each upstream model that has data from the dictionary variable and generate a union statement #}
    {%- for item in upstream_models if upstream_models[item].has_data  -%}

        SELECT * FROM {{ ref(upstream_models[item].model_ref) }}

        {% if not loop.last %} UNION ALL {% endif %}

    {%- endfor -%}
    
),

final AS (

    SELECT

        {{ dbt_utils.surrogate_key(['date', 'account_id']) }} AS id,
        *
    
    FROM union_tables
)

SELECT * FROM final