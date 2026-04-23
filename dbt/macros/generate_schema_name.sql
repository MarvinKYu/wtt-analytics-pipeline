-- generate_schema_name.sql
-- Overrides dbt's default schema naming to use the target schema directly
-- (ignores the default schema prefix dbt appends in multi-env setups).

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
