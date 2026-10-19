{#
    Uses a model's custom schema exactly as configured (e.g. `staging`), instead of
    dbt's default `<target schema>_<custom schema>`. The warehouse schemas are fixed by
    the platform and the dbt user cannot create new ones.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
