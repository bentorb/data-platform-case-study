-- Sales per product, over all transactions.

select
    fct.product_id,
    dim_products.product_name,
    count(*) as transactions,
    sum(fct.quantity) as units,
    count(distinct fct.customer_id) as customers,
    sum(fct.net_amount) as net_amount,
    sum(fct.gross_amount) as gross_amount,
    -- Weighted by units; a plain average of prices would overweight small orders.
    round(sum(fct.net_amount) / sum(fct.quantity), 2) as avg_unit_price,
    min(fct.transaction_date) as first_sale_date,
    max(fct.transaction_date) as last_sale_date
from {{ ref('fct_transactions') }} as fct
inner join {{ ref('dim_products') }} as dim_products using (product_id)
group by fct.product_id, dim_products.product_name
