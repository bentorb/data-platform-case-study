-- Transactions whose product name differs from the dim_products version covering their
-- date: late or corrected names that the incremental dimension did not apply, or two
-- names on the same day. Warns: a full refresh of dim_products+ applies late names.

{{ config(severity='warn') }}

select
    stg.transaction_id,
    stg.transaction_date,
    stg.product_id,
    stg.product_name,
    dim_products.product_name as dim_product_name,
    dim_products.product_key,
    stg._load_id,
    stg._source_file,
    stg._source_row
from {{ ref('stg_customer_transactions') }} as stg
inner join {{ ref('dim_products') }} as dim_products
    on dim_products.product_id = stg.product_id
   and stg.transaction_date >= dim_products.valid_from
   and stg.transaction_date < dim_products.valid_to
where stg.product_name <> dim_products.product_name
