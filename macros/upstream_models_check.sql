{%- macro upstream_models_check(relations) -%}

{#- Create list variable of upstream models that could be used in the rollup -#}
{%- set upstream_models = relations -%}

{#- When ref() is placed within a conditional block, dbt needs to infer all dependencies for this model. To fix this, need the following hint at the top of the model -#}
-- depends_on: {% for model in upstream_models -%} {{ ref(model) }} {%- if not loop.last %},{% endif %} {% endfor %}

{#-  Since this model is using the dbt "graph" variable, then we need to use the execute flag to ensure that this code only executes at run-time and not at parse-time.
See this for more information: https://docs.getdbt.com/reference/dbt-jinja-functions/graph/#accessing-models -#}
{% if execute -%}

    {#- Create list variable of enabled upstream models relations -#}
    {% set enabled_upstream_models = [] -%}
    {% for node in graph.nodes.values() -%}
        {%- if node.name in upstream_models and node.config.enabled -%}
            {%- do enabled_upstream_models.append(ref(node.name)) -%}
        {%- endif -%}
    {%- endfor %}

    {{ return(enabled_upstream_models) }}

{%- endif %}

{%- endmacro -%}