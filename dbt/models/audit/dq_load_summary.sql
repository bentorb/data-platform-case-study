-- One row per load and staging model: how many rows had data-quality issues. Each run
-- adds the loads not recorded yet, so the history of replaced loads is kept.
-- To monitor another staging model, add it to the list.

{% set staging_models = ['stg_customer_transactions'] %}

{% for staging_model in staging_models %}

select
    _load_id as load_id,
    '{{ staging_model }}' as model,
    count(*) as total_rows,
    count(*) filter (where cardinality(dq_errors) > 0) as rows_with_errors,
    count(*) filter (where cardinality(dq_warnings) > 0) as rows_with_warnings,
    -- Cast to the contract's type: incremental runs compare types exactly.
    (count(*) filter (where cardinality(dq_errors) > 0)::numeric / count(*))::numeric(5, 4) as error_rate,
    now() as measured_at
from {{ ref(staging_model) }}
{% if is_incremental() %}
where _load_id not in (select load_id from {{ this }} where model = '{{ staging_model }}')
{% endif %}
group by _load_id

{% if not loop.last %}union all{% endif %}
{% endfor %}
