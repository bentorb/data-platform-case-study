{#
    Model-level test for staging models: the share of rows with data-quality errors
    must not exceed `threshold` (0 to 1). When it does, returns one row with the
    measured rate. Usually run with severity warn: a high rate means incomplete data
    downstream, not wrong data.
#}
{% test max_error_rate(model, threshold, errors_column='dq_errors') %}

with counts as (

    select
        count(*) as total_rows,
        count(*) filter (where cardinality({{ errors_column }}) > 0) as rows_with_errors
    from {{ model }}

)

select
    total_rows,
    rows_with_errors,
    round(rows_with_errors::numeric / total_rows, 4) as error_rate,
    {{ threshold }} as threshold
from counts
where total_rows > 0
  and rows_with_errors::numeric / total_rows > {{ threshold }}

{% endtest %}
