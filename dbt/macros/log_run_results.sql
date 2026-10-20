{#
    Records the result of every model and test of a dbt invocation in
    audit.dbt_run_results. Runs as the on-run-end hook (dbt_project.yml), which dbt
    calls with `results` after run, test and build commands. Airflow's AIRFLOW_CTX_*
    environment variables identify the task attempt when dbt runs inside one.
#}
{% macro log_run_results(results) %}
    {%- if execute and results -%}
        {%- set airflow = airflow_task_attempt() -%}
        {%- set rows = [] -%}
        {%- for result in results -%}
            {#- rows_affected only for models: for tests, the adapter response is
                about storing the failures. -#}
            {%- set values = [
                sql_text_literal(invocation_id),
                sql_text_literal(flags.WHICH),
                sql_text_literal(run_started_at.isoformat()) ~ '::timestamptz',
                sql_text_literal(result.node.unique_id),
                sql_text_literal(result.node.resource_type),
                sql_text_literal(result.status),
                result.failures if result.failures is not none else 'null',
                result.adapter_response.get('rows_affected', 'null')
                    if result.node.resource_type == 'model' and result.adapter_response else 'null',
                result.execution_time | round(3),
                sql_text_literal(result.message),
                airflow,
            ] -%}
            {%- do rows.append('(' ~ values | join(', ') ~ ')') -%}
        {%- endfor -%}
        insert into audit.dbt_run_results (
            invocation_id, command, run_started_at, node_id, resource_type, status,
            failures, rows_affected, execution_time, message,
            dag_id, run_id, task_id, try_number
        ) values
        {{ rows | join(',\n') }}
    {%- endif -%}
{% endmacro %}


{# A SQL text literal, or null for empty values. #}
{% macro sql_text_literal(value) -%}
    {%- if value is none or value | string == '' -%}
        null
    {%- else -%}
        '{{ value | string | replace("'", "''") }}'
    {%- endif -%}
{%- endmacro %}


{#
    The Airflow task attempt running dbt, as SQL values for dag_id, run_id, task_id
    and try_number: null when dbt runs outside Airflow.
#}
{% macro airflow_task_attempt() -%}
    {%- set try_number = env_var('AIRFLOW_CTX_TRY_NUMBER', '') -%}
    {{ return([
        sql_text_literal(env_var('AIRFLOW_CTX_DAG_ID', '')),
        sql_text_literal(env_var('AIRFLOW_CTX_DAG_RUN_ID', '')),
        sql_text_literal(env_var('AIRFLOW_CTX_TASK_ID', '')),
        try_number if try_number else 'null',
    ] | join(', ')) }}
{%- endmacro %}
