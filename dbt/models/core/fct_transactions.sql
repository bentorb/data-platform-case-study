-- One row per customer transaction. Only transactions that passed every staging
-- check are included; the rest stay in staging with their dq_errors. Tax is per
-- unit, like price (an assumption: the source does not say).
--
-- Incremental: each run adds the valid rows of the loads since the last run, after
-- deleting the rows of files those loads delivered again (macros/incremental_loads.sql).
-- A full refresh rebuilds the same table from all of raw; a full refresh of
-- dim_products, which can change its keys, must include this model (dim_products+). The indexes, created with
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
    stg.transaction_id,
    stg.transaction_date,
    stg.customer_id,
    stg.product_id,
    dim_products.product_key,
    stg.quantity,
    stg.price as unit_price,
    stg.tax as unit_tax,
    (stg.quantity * stg.price)::numeric(14, 2) as net_amount,
    (stg.quantity * stg.tax)::numeric(14, 2) as tax_amount,
    (stg.quantity * (stg.price + stg.tax))::numeric(14, 2) as gross_amount,
    stg._load_id,
    stg._source_file,
    stg._source_row
from {{ ref('stg_customer_transactions') }} as stg
-- The product's version on the transaction's date. A left join, so a transaction
-- without one fails the not-null constraint instead of being dropped.
left join {{ ref('dim_products') }} as dim_products
    on stg.product_id = dim_products.product_id
        and stg.transaction_date >= dim_products.valid_from
        and stg.transaction_date < dim_products.valid_to
where cardinality(stg.dq_errors) = 0
    {% if is_incremental() %}
        and stg._load_id > {{ load_watermark() }}
    {% endif %}
