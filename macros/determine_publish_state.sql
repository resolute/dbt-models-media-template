{%- macro determine_publish_state(account_ids=[]) -%}

{% if execute %}

    {% set upstream_models = model.depends_on.nodes|unique|list -%}

    {#- Create list variable of used/non-empty upstream models relations -#}
    {% set used_upstream_models = [] -%}
    
    {% for upstream_model in upstream_models -%}
        {% for source in graph.sources if source.database == upstream_model.database and source.schema == upstream_model.schema and source.identifier == upstream_model.identifier -%}
            {%- if account_ids != [] and account_ids != None -%}
                {% set source_account_ids_check_query %}
                    select distinct account_id
                    from {{ source(upstream_model.source_name, upstream_model.name) }}
                    where account_id IN UNNEST({{ account_ids }})
                {% endset %}
                {% set results = run_query(source_account_ids_check_query) %}
                {% set results_list = results.columns[0].values() %}
                {%- if results_list|length > 0 -%}
                    {%- do used_upstream_models.append(upstream_model) -%}
                {%- endif -%}
            {%- endif -%}
        {% else %}
            {%- if "unpublished" not in upstream_model.schema -%}
                {%- do used_upstream_models.append(upstream_model) -%}
            {%- endif -%}
        {%- endfor %}
    {%- endfor %}
{% endif %}

{%- if used_upstream_models|length > 0 -%}
    {{ return('published') }}
{% else %}
    {{ return('unpublished') }}
{%- endif -%}

{%- endmacro  -%}