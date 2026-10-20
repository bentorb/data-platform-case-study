{#
    Records a staging model's data-quality issues in audit.dq_load_summary and
    audit.dq_issue_counts, for its loads not recorded yet. Runs as a post-hook of every
    staging model (dbt_project.yml), so each pipeline records the issues of its own
    loads when it builds its staging model, inside the same transaction.

    The model must have the staging columns _load_id, dq_errors and dq_warnings.
#}

{% macro record_dq_issues(model) %}

insert into audit.dq_load_summary
    (load_id, model, total_rows, rows_with_errors, rows_with_warnings, error_rate)
select
    _load_id,
    {{ sql_text_literal(model.identifier) }},
    count(*),
    count(*) filter (where cardinality(dq_errors) > 0),
    count(*) filter (where cardinality(dq_warnings) > 0),
    round(count(*) filter (where cardinality(dq_errors) > 0)::numeric / count(*), 4)
from {{ model }}
where _load_id not in (
    select load_id from audit.dq_load_summary where model = {{ sql_text_literal(model.identifier) }}
)
group by _load_id;

insert into audit.dq_issue_counts (load_id, model, kind, code, affected_rows)
select
    staged._load_id,
    {{ sql_text_literal(model.identifier) }},
    issues.kind,
    issues.code,
    count(*)
from {{ model }} as staged
cross join lateral (
    select 'error' as kind, code from unnest(staged.dq_errors) as code
    union all
    select 'warning' as kind, code from unnest(staged.dq_warnings) as code
) as issues
where staged._load_id not in (
    select load_id from audit.dq_issue_counts where model = {{ sql_text_literal(model.identifier) }}
)
group by staged._load_id, issues.kind, issues.code

{% endmacro %}
