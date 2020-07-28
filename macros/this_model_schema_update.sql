{%- macro this_model_schema_update(model_name, custom_schema, account_ids) -%}

{% if execute %}
    {% for node in graph.nodes.values() if node.name == 'stg_youtube_organic__traffic_source_daily' -%}
        {% do log(node.depends_on.nodes|unique|list, info=true) %}
    {%- endfor %}

{#- Create list variable of used/non-empty upstream models relations -#}
{% set used_upstream_models = [] -%}
{% for relation in relations -%}
    {% for source in graph.sources.values() if source.database == relation.database and source.schema == relation.schema and source.identifier == relation.identifier -%}
        {%- if account_ids != [] and account_ids != None -%}
            {% set source_account_ids_check_query %}
                select distinct account_id
                from {{ relation }}
                where account_id IN UNNEST({{ account_ids }})
            {% endset %}
            {% set results = run_query(source_account_ids_check_query) %}
            {% set results_list = results.columns[0].values() %}
            {%- if results_list|length > 0 -%}
                {%- do used_upstream_models.append(relation) -%}
            {%- endif -%}
        {%- endif -%}
    {% else %}
        {%- if "unused_models" not in relation.schema -%}
            {%- do used_upstream_models.append(relation) -%}
        {%- endif -%}
    {%- endfor %}
{%- endfor %}
{% endif %}
{%- if used_upstream_models|length > 0 -%}
    {{-
        config(
            schema = custom_schema
        )
    -}}
{% else %}
    {{-
        config(
            schema = custom_schema ~ '_unused_models'
        )
    -}}
{%- endif -%}

{%- endmacro  -%}