{#
    Fails when a model's totals differ from another model's, e.g. an aggregate's from
    the fact it aggregates. `totals` maps each column of the model, summed, to the
    expression it must equal over `compare_to`. Returns both sets of totals.
#}

{% test totals_match(model, compare_to, totals) %}

with model_totals as (

    select
        {%- for column in totals %}
        sum({{ column }}) as {{ column }}{{ ',' if not loop.last }}
        {%- endfor %}
    from {{ model }}

),

expected_totals as (

    select
        {%- for column, expression in totals.items() %}
        {{ expression }} as expected_{{ column }}{{ ',' if not loop.last }}
        {%- endfor %}
    from {{ compare_to }}

)

select *
from model_totals
cross join expected_totals
where ({{ totals | join(', ') }})
      is distinct from ({% for column in totals %}expected_{{ column }}{{ ', ' if not loop.last }}{% endfor %})

{% endtest %}
