{#- Create list variable of upstream models that could be used in the rollup -#}
{%- set upstream_models = ['facebook_ads__creatives_daily', 'linkedin_ads__creatives_daily'] -%}

{#- Create list variable of enabled upstream models relations -#}
{% set enabled_upstream_models = [] -%}
{% for node in graph.nodes.values() -%}
    {%- if node.name in upstream_models and node.config.enabled -%}
        {%- do enabled_upstream_models.append(ref(node.name)) -%}
    {%- endif -%}
{%- endfor %}

{#- Enable this model only if any of the upstream models are enabled -#}
{{-
    config(
        enabled = enabled_upstream_models|length > 0
    )
-}}

WITH

union_tables AS (

{{ dbt_utils.union_relations(
    relations= enabled_upstream_models,
    exclude= ['id']
) }}

),

final AS (

    SELECT
    
        {{ dbt_utils.surrogate_key(['date', 'account_id', 'creative_id']) }} AS id,
        *
    
    FROM union_tables
)

SELECT * FROM final