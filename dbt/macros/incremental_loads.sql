{#
    For incremental models built from raw loads. Each run processes the loads that
    arrived since the last run, and the file is the unit that gets replaced, as in
    raw: a file delivered again replaces the rows of its earlier load.
#}

{#
    The latest load in the model; loads after it are new. 0 while the model has no rows.
    Assumes a table's loads commit in load_id order: a load committed after a later one
    would be below the watermark, and never processed. True while one pipeline loads
    each raw table, as its load tasks run one after another (depends_on_past, see
    lib/pipelines/builder.py).
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
