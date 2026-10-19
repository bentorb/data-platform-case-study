-- Sales per day, with how complete each day's figures are: transactions rejected in
-- staging are missing from the sales columns, and counted in rejected_transactions.
-- Rejected rows whose date could not be parsed cannot be assigned to a day.

with sales as (

    select
        transaction_date,
        count(*) as transactions,
        sum(quantity) as units,
        count(distinct customer_id) as customers,
        sum(net_amount) as net_amount,
        sum(tax_amount) as tax_amount,
        sum(gross_amount) as gross_amount
    from {{ ref('fct_transactions') }}
    group by transaction_date

),

rejected as (

    select
        transaction_date,
        count(*) as rejected_transactions
    from {{ ref('stg_customer_transactions') }}
    where cardinality(dq_errors) > 0
      and transaction_date is not null
    group by transaction_date

)

-- Full join: a day may have only rejected transactions.
select
    transaction_date,
    coalesce(sales.transactions, 0) as transactions,
    coalesce(sales.units, 0) as units,
    coalesce(sales.customers, 0) as customers,
    coalesce(sales.net_amount, 0) as net_amount,
    coalesce(sales.tax_amount, 0) as tax_amount,
    coalesce(sales.gross_amount, 0) as gross_amount,
    coalesce(rejected.rejected_transactions, 0) as rejected_transactions,
    round(
        coalesce(sales.transactions, 0)::numeric
            / (coalesce(sales.transactions, 0) + coalesce(rejected.rejected_transactions, 0)),
        4
    ) as completeness
from sales
full join rejected using (transaction_date)
