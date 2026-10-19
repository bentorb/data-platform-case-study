-- One row per customer transaction. Only transactions that passed every staging
-- check are included; the rest stay in staging with their dq_errors. Tax is per
-- unit, like price (an assumption: the source does not say).
--
-- Incremental: each run adds the valid rows of the loads since the last run, after
-- deleting the rows of files those loads delivered again (macros/incremental_loads.sql).
-- A full refresh rebuilds the same table from all of raw. The indexes, created with
-- the table, keep the pre-hook's delete and the watermark fast.

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='fail',
        pre_hook="{{ delete_redelivered_files(ref('stg_customer_transactions')) }}",
        indexes=[
            {'columns': ['_source_file']},
            {'columns': ['_load_id']},
        ],
    )
}}

select
    transaction_id,
    transaction_date,
    customer_id,
    product_id,
    quantity,
    price as unit_price,
    tax as unit_tax,
    (quantity * price)::numeric(14, 2) as net_amount,
    (quantity * tax)::numeric(14, 2) as tax_amount,
    (quantity * (price + tax))::numeric(14, 2) as gross_amount,
    _load_id,
    _source_file,
    _source_row
from {{ ref('stg_customer_transactions') }}
where cardinality(dq_errors) = 0
{% if is_incremental() %}
    and _load_id > {{ load_watermark() }}
{% endif %}
