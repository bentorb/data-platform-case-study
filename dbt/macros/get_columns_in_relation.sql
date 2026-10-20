{#
    Overrides dbt-postgres's macro to report array columns with their element type, e.g.
    text[], instead of information_schema's bare ARRAY, which is not a valid type: dbt
    casts unit test inputs and expected rows to these types, and `cast(... as ARRAY)`
    fails. Every other type is reported exactly as by dbt-postgres 1.11.
#}
{% macro postgres__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          case
              when data_type = 'ARRAY' then substring(udt_name from 2) || '[]'
              else data_type
          end as data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale

      from {{ relation.information_schema('columns') }}
      where table_name = '{{ relation.identifier }}'
        {% if relation.schema %}
        and table_schema = '{{ relation.schema }}'
        {% endif %}
      order by ordinal_position

  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}
