{#
    Model-level test for staging models: the share of rows with data-quality errors in
    the model's newest load must not exceed `threshold` (0 to 1). When it does, returns
    the load with its measured rate. Usually run with severity warn: a high rate means
    incomplete data downstream, not wrong data.

    The newest load, not all of history: a bad delivery shows the day it arrives,
    instead of barely moving a rate over years of loads. The rate is read from the
    data-quality history (audit.dq_load_summary), which the staging model's post-hook
    has just written, so the test reads one row rather than all of the model.
#}
{% test max_error_rate(model, threshold) %}

with newest as (

    select load_id, total_rows, rows_with_errors, error_rate
    from {{ source('audit', 'dq_load_summary') }}
    where model = '{{ model.identifier }}'
    order by load_id desc
    limit 1

)

select
    load_id,
    total_rows,
    rows_with_errors,
    error_rate,
    {{ threshold }} as threshold
from newest
where error_rate > {{ threshold }}

{% endtest %}
