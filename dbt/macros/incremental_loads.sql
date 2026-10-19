{#
    For incremental models built from raw loads. Each run processes the loads that
    arrived since the last run, and the file is the unit that gets replaced, as in
    raw: a file delivered again replaces the rows of its earlier load.
#}

{#
    The latest load in the model; loads after it are new. 0 while the model has no rows.
    Assumes loads commit in load_id order, true while one load runs at a time
    (max_active_runs=1, one load task).
#}
{% macro load_watermark() %}
    (select coalesce(max(_load_id), 0) from {{ this }})
{% endmacro %}

{#
    Pre-hook: deletes the rows of files that new loads delivered again, so their rows
    are replaced rather than kept twice, and rows a new delivery dropped or that now
    fail a check are removed. `source_model` must be unfiltered by data quality, so
    a file whose rows are now all rejected is still found.
#}
{% macro delete_redelivered_files(source_model) %}
{% if is_incremental() %}
delete from {{ this }}
where _source_file in (
    select _source_file
    from {{ source_model }}
    where _load_id > {{ load_watermark() }}
)
{% endif %}
{% endmacro %}
