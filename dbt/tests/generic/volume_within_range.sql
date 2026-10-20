{#
    Fails when a staging model's newest load has far more or far fewer rows than usual:
    a half-empty file, or one with ten times the usual rows, is often an upstream
    problem even when every row is valid. "Usual" is the median row count of the
    `lookback` loads before it, from the data-quality history (audit.dq_load_summary),
    and far is more than `tolerance` (a share of the median) away from it. Passes while
    there are fewer than `min_history` earlier loads, so a new pipeline does not fail
    on its first days. Returns the newest load, its rows and the expected range.
#}

{% test volume_within_range(model, lookback=14, min_history=7, tolerance=0.5) %}

with loads as (

    select load_id, total_rows
    from {{ source('audit', 'dq_load_summary') }}
    where model = '{{ model.identifier }}'

),

newest as (

    select load_id, total_rows
    from loads
    order by load_id desc
    limit 1

),

history as (

    select total_rows
    from loads
    where load_id < (select newest.load_id from newest)
    order by load_id desc
    limit {{ lookback }}

),

expected as (

    select
        count(*) as history_loads,
        percentile_cont(0.5) within group (order by total_rows) as median_rows
    from history

)

select
    newest.load_id,
    newest.total_rows,
    expected.history_loads,
    expected.median_rows,
    round((expected.median_rows * (1 - {{ tolerance }}))::numeric, 1) as min_rows,
    round((expected.median_rows * (1 + {{ tolerance }}))::numeric, 1) as max_rows
from newest
cross join expected
where expected.history_loads >= {{ min_history }}
    and (
        newest.total_rows < expected.median_rows * (1 - {{ tolerance }})
        or newest.total_rows > expected.median_rows * (1 + {{ tolerance }})
    )

{% endtest %}
