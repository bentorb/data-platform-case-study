-- One row per raw customer transaction. Exposes the raw columns as they are;
-- cleaning and typing come later.

select
    transaction_id,
    customer_id,
    transaction_date,
    product_id,
    product_name,
    quantity,
    price,
    tax,
    _source_file,
    _source_row,
    _loaded_at,
    _load_id
from {{ source('raw', 'customer_transactions') }}
