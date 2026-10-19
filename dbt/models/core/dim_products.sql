-- One row per product, with its name from the most recent transaction, so a rename
-- resolves deterministically. Products are derived from transactions because there is
-- no product master source.

select distinct on (product_id)
    product_id,
    product_name
from {{ ref('stg_customer_transactions') }}
where product_id is not null
  and product_name is not null
order by product_id, transaction_date desc nulls last, _loaded_at desc, _source_row desc
