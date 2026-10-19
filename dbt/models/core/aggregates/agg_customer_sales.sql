-- Sales per customer, over all transactions.

select
    customer_id,
    count(*) as transactions,
    sum(quantity) as units,
    count(distinct product_id) as products,
    sum(net_amount) as net_amount,
    sum(gross_amount) as gross_amount,
    round(sum(gross_amount) / count(*), 2) as avg_transaction_value,
    min(transaction_date) as first_transaction_date,
    max(transaction_date) as last_transaction_date
from {{ ref('fct_transactions') }}
group by customer_id
