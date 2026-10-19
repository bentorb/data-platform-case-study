-- One row per load, staging model and issue code: how many rows had the issue. Each
-- run adds the loads not recorded yet, so the history of replaced loads is kept.
-- To monitor another staging model, add it to the list.

{% set staging_models = ['stg_customer_transactions'] %}

{% for staging_model in staging_models %}

select
    staged._load_id as load_id,
    '{{ staging_model }}' as model,
    issues.kind,
    issues.code,
    count(*) as affected_rows
from {{ ref(staging_model) }} as staged
cross join lateral (
    select 'error' as kind, code from unnest(staged.dq_errors) as code
    union all
    select 'warning' as kind, code from unnest(staged.dq_warnings) as code
) as issues
{% if is_incremental() %}
where staged._load_id not in (select load_id from {{ this }} where model = '{{ staging_model }}')
{% endif %}
group by staged._load_id, issues.kind, issues.code

{% if not loop.last %}union all{% endif %}
{% endfor %}
