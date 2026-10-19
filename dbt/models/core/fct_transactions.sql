-- One row per customer transaction. Only transactions that passed every staging
-- check are included; the rest stay in staging with their dq_errors. Tax is per
-- unit, like price (an assumption: the source does not say).

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
