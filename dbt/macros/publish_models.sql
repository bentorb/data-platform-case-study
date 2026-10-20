{#
    Publishes core models to live once they have passed their tests: makes each
    live table equal to its core model, in one transaction for all the models given,
    so consumers see either the previous or the new version of all of them. Run by
    each pipeline's publish task (lib/transformation/dbt.py), after its dbt tests.

    Live tables are created from their core model on first publish, and never dropped
    or replaced afterwards, only their rows change: views that consumers build on them
    keep working. Rows are matched on the model's primary key (from its contract):
    rows that changed or disappeared are deleted, then changed and new rows inserted.

    Models outside `source_schema` are ignored, so a pipeline can pass all its models.
#}

{% macro publish_models(models, source_schema='core', target_schema='live') %}

{#- Check every model before changing anything, so a problem fails the whole publish early. -#}
{%- set plan = [] -%}
{%- for node in graph.nodes.values()
        if node.resource_type == 'model' and node.name in models and node.schema == source_schema -%}
    {%- set source = adapter.get_relation(node.database, node.schema, node.alias) -%}
    {%- if source is none -%}
        {{ exceptions.raise_compiler_error("Cannot publish " ~ node.name ~ ": " ~ node.schema ~ "." ~ node.alias ~ " does not exist") }}
    {%- endif -%}
    {%- set existing = adapter.get_relation(node.database, target_schema, node.alias) -%}
    {%- if existing is not none -%}
        {%- do publish_check_columns(source, existing) -%}
    {%- endif -%}
    {%- do plan.append({
        'name': node.name,
        'source': source,
        'target': api.Relation.create(database=node.database, schema=target_schema, identifier=node.alias, type='table'),
        'exists': existing is not none,
        'key': publish_primary_key(node) | join(', '),
    }) -%}
{%- endfor -%}

{#-
    One script, run as one transaction: consumers see the previous or the new version
    of every model, and an error anywhere rolls the whole publish back. (Separate
    statements would each be committed on their own by dbt run-operation.)
-#}
{%- set statements = ['begin'] -%}
{%- for model in plan -%}
    {%- if not model.exists -%}
        {%- do statements.append("create table " ~ model.target ~ " (like " ~ model.source ~ " including all)") -%}
    {%- endif -%}
    {#- Rows that changed or disappeared, then rows that changed or are new. -#}
    {%- do statements.append(
        "delete from " ~ model.target ~ " where (" ~ model.key ~ ") in ("
        ~ "select " ~ model.key ~ " from (select * from " ~ model.target
        ~ " except select * from " ~ model.source ~ ") as changed)"
    ) -%}
    {%- do statements.append(
        "insert into " ~ model.target ~ " select * from " ~ model.source ~ " except select * from " ~ model.target
    ) -%}
{%- endfor -%}
{%- do statements.append('commit') -%}
{%- call statement('publish_models') -%}
    {{ statements | join(';\n') }}
{%- endcall -%}

{%- do log("Published " ~ (plan | length) ~ " models to " ~ target_schema ~ ": "
           ~ (plan | map(attribute='name') | join(', ')), info=true) -%}

{% endmacro %}


{# The primary key columns of a model, from its contract's constraints. #}
{% macro publish_primary_key(node) %}
    {%- set key = [] -%}
    {%- for constraint in node.constraints if constraint.type == 'primary_key' -%}
        {%- do key.extend(constraint.columns) -%}
    {%- endfor -%}
    {%- for column in node.columns.values() -%}
        {%- for constraint in column.constraints if constraint.type == 'primary_key' -%}
            {%- do key.append(column.name) -%}
        {%- endfor -%}
    {%- endfor -%}
    {%- if not key -%}
        {{ exceptions.raise_compiler_error("Cannot publish " ~ node.name ~ ": it has no primary key in its contract") }}
    {%- endif -%}
    {{ return(key) }}
{% endmacro %}


{#
    Fails when the live table's columns differ from the model's: rows cannot be synced,
    and replacing the table would drop the views built on it, so it is migrated
    deliberately instead.
#}
{% macro publish_check_columns(source, target) %}
    {%- set describe = [] -%}
    {%- for relation in [source, target] -%}
        {%- set columns = [] -%}
        {%- for column in adapter.get_columns_in_relation(relation) -%}
            {%- do columns.append(column.name ~ ' ' ~ column.data_type) -%}
        {%- endfor -%}
        {%- do describe.append(columns | join(', ')) -%}
    {%- endfor -%}
    {%- if describe[0] != describe[1] -%}
        {{ exceptions.raise_compiler_error(
            "Cannot publish " ~ source ~ ": " ~ target ~ " has different columns. Migrate it first.\n"
            ~ "  " ~ source ~ ": " ~ describe[0] ~ "\n  " ~ target ~ ": " ~ describe[1]
        ) }}
    {%- endif -%}
{% endmacro %}
