{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {# determine the parent folder from node.fqn (elements between package and model name) #}
    {%- set fqn = node.fqn -%}
    {%- set folders = fqn[1:-1] -%}
    {%- if folders | length > 0 -%}
        {%- set parent = folders[-1] -%}
    {%- else -%}
        {%- set parent = fqn[1] -%}
    {%- endif -%}


    {{ parent | trim }}

{%- endmacro %}