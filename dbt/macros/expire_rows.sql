{#
    Deletes the rows of a core model older than its retention, declared on the model as
    config.meta.retention: {column: <date column>, days: <days kept>}. Runs as a
    post-hook of every core model (dbt_project.yml), inside the model's transaction:
    models downstream, tests and the publish to live only ever see the expired data,
    even when an old file is reprocessed. Models without a retention are kept forever,
    and get no statement. Each expiry is recorded in audit.retention_log.
#}

{% macro expire_rows(relation) %}
    {%- set retention = (model.config.meta or {}).get('retention') -%}
    {%- if retention -%}
        {%- set cutoff = "now() - make_interval(days => " ~ (retention.days | int) ~ ")" -%}
        with deleted as (
            delete from {{ relation }}
            where {{ adapter.quote(retention.column) }} < {{ cutoff }}
            returning 1
        )
        insert into audit.retention_log
            (target_table, cutoff, rows_deleted, dag_id, run_id, task_id, try_number)
        select {{ sql_text_literal(relation.schema ~ '.' ~ relation.identifier) }}, {{ cutoff }}, count(*),
               {{ airflow_task_attempt() }}
        from deleted
    {%- endif -%}
{% endmacro %}
