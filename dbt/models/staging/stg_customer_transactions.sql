-- One row per raw customer transaction, typed and cleaned. Values in a known
-- alternative format are repaired and listed in dq_warnings; values that cannot be
-- used are set to null and listed in dq_errors. No row is dropped here.

with source as (

    select
        nullif(trim(transaction_id), '') as raw_transaction_id,
        nullif(trim(customer_id), '') as raw_customer_id,
        nullif(trim(transaction_date), '') as raw_transaction_date,
        nullif(trim(product_id), '') as raw_product_id,
        nullif(trim(product_name), '') as raw_product_name,
        nullif(trim(quantity), '') as raw_quantity,
        nullif(trim(price), '') as raw_price,
        nullif(trim(tax), '') as raw_tax,
        _source_file,
        _source_row,
        _loaded_at,
        _load_id
    from {{ source('raw', 'customer_transactions') }}

),

parsed as (

    select
        *,
        {{ parse_integer('raw_transaction_id', prefix='T') }} as transaction_id,
        {{ parse_integer('raw_customer_id') }} as customer_id,
        {{ parse_date('raw_transaction_date') }} as transaction_date,
        {{ parse_integer('raw_product_id', prefix='P') }} as product_id,
        raw_product_name as product_name,
        {{ parse_integer('raw_quantity') }} as quantity,
        {{ parse_decimal('raw_price') }} as price,
        {{ parse_decimal('raw_tax') }} as tax
    from source

)

select
    transaction_id,
    customer_id,
    transaction_date,
    product_id,
    product_name,
    quantity,
    price,
    tax,
    {{ dq_codes([
        "case when raw_transaction_id like 'T%' and transaction_id is not null then 'transaction_id_prefixed' end",
        "case when raw_transaction_date ~ '^[0-9]{2}-' and transaction_date is not null then 'transaction_date_dmy' end",
        "case when raw_product_id like 'P%' and product_id is not null then 'product_id_prefixed' end",
    ]) }} as dq_warnings,
    {{ dq_codes([
        dq_error('transaction_id'),
        dq_error('customer_id'),
        dq_error('transaction_date'),
        dq_error('product_id'),
        dq_error('product_name'),
        dq_error('quantity'),
        dq_error('price'),
        dq_error('tax'),
    ]) }} as dq_errors,
    _source_file,
    _source_row,
    _loaded_at,
    _load_id
from parsed
