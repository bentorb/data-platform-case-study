{#
    A lock per model, so that two builds of the same model never run at once (dbt's
    table builds would collide on their __dbt_tmp and __dbt_backup tables, and two
    incremental builds could both process the same loads). A second build waits until
    the first has finished, then builds on its result; builds of different models never
    wait for each other. Taken and released by hooks outside the model's transaction
    (dbt_project.yml), so the lock also covers what dbt does after committing, such as
    dropping the backup table. A failed build's lock is released when its connection
    closes, at the end of its dbt process.

    Postgres advisory locks, named after the model's relation: only builds, and the
    publish (macros/publish_models.sql), take them.
#}

{% macro model_lock_key(relation) -%}
    hashtextextended('{{ relation.schema }}.{{ relation.identifier }}', 0)
{%- endmacro %}

{% macro lock_model(relation) -%}
    select pg_advisory_lock({{ model_lock_key(relation) }})
{%- endmacro %}

{% macro unlock_model(relation) -%}
    select pg_advisory_unlock({{ model_lock_key(relation) }})
{%- endmacro %}
