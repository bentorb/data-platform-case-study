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
    Each published model is recorded in audit.publish_log, in the same transaction.

    Access follows each column's classification (config.meta.classification in the
    model's YAML): `reader_role` may read the columns classified `internal`, the
    default, and `restricted_reader_role` every column. The grants are applied with
    every publish, so a reclassified column takes effect on the next one.
#}

{% macro publish_models(models, source_schema='core', target_schema='live',
                        reader_role='analytics', restricted_reader_role='analytics_restricted') %}

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
        'columns': node.columns.keys() | list,
        'internal_columns': publish_internal_columns(node),
    }) -%}
{%- endfor -%}

{#-
    One script, run as one transaction: consumers see the previous or the new version
    of every model, and an error anywhere rolls the whole publish back. (Separate
    statements would each be committed on their own by dbt run-operation.)
-#}
{%- set statements = ['begin'] -%}
{%- for model in plan -%}
    {%- set log_key = "invocation_id = " ~ sql_text_literal(invocation_id) ~ " and model = " ~ sql_text_literal(model.name) -%}
    {%- if not model.exists -%}
        {%- do statements.append("create table " ~ model.target ~ " (like " ~ model.source ~ " including all)") -%}
    {%- endif -%}
    {#- Rows that changed or disappeared, then rows that changed or are new, each
        counted into the publish log. Two statements, so the insert sees the delete. -#}
    {%- do statements.append(
        "with deleted as (delete from " ~ model.target ~ " where (" ~ model.key ~ ") in ("
        ~ "select " ~ model.key ~ " from (select * from " ~ model.target
        ~ " except select * from " ~ model.source ~ ") as changed) returning 1) "
        ~ "insert into audit.publish_log (invocation_id, model, target_table, created, rows_deleted,"
        ~ " dag_id, run_id, task_id, try_number) select "
        ~ sql_text_literal(invocation_id) ~ ", " ~ sql_text_literal(model.name) ~ ", "
        ~ sql_text_literal(model.target.schema ~ '.' ~ model.target.identifier) ~ ", "
        ~ (not model.exists) ~ ", count(*), " ~ airflow_task_attempt() ~ " from deleted"
    ) -%}
    {%- do statements.append(
        "with inserted as (insert into " ~ model.target ~ " select * from " ~ model.source
        ~ " except select * from " ~ model.target ~ " returning 1) "
        ~ "update audit.publish_log set rows_inserted = (select count(*) from inserted) where " ~ log_key
    ) -%}
    {#- Access by classification: first remove every earlier grant to the reader role,
        table-level and column-level, then grant the internal columns. -#}
    {%- do statements.append("revoke select on " ~ model.target ~ " from " ~ reader_role) -%}
    {%- do statements.append(
        "revoke select (" ~ model.columns | join(', ') ~ ") on " ~ model.target ~ " from " ~ reader_role
    ) -%}
    {%- if model.internal_columns -%}
        {%- do statements.append(
            "grant select (" ~ model.internal_columns | join(', ') ~ ") on " ~ model.target ~ " to " ~ reader_role
        ) -%}
    {%- endif -%}
    {%- do statements.append("grant select on " ~ model.target ~ " to " ~ restricted_reader_role) -%}
{%- endfor -%}
{%- do statements.append('commit') -%}
{%- call statement('publish_models') -%}
    {{ statements | join(';\n') }}
{%- endcall -%}

{%- do log("Published " ~ (plan | length) ~ " models to " ~ target_schema ~ ": "
           ~ (plan | map(attribute='name') | join(', ')), info=true) -%}

{% endmacro %}


{#
    The columns of a model classified `internal`: those without a classification, or
    with `classification: internal`. Any other value than internal or restricted fails.
#}
{% macro publish_internal_columns(node) %}
    {%- set internal = [] -%}
    {%- for column in node.columns.values() -%}
        {%- set meta = column.config.meta if column.config and column.config.meta else column.meta or {} -%}
        {%- set classification = meta.get('classification', 'internal') -%}
        {%- if classification not in ['internal', 'restricted'] -%}
            {{ exceptions.raise_compiler_error(
                "Cannot publish " ~ node.name ~ ": column " ~ column.name ~ " has classification '"
                ~ classification ~ "', expected internal or restricted"
            ) }}
        {%- endif -%}
        {%- if classification == 'internal' -%}
            {%- do internal.append(column.name) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(internal) }}
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
