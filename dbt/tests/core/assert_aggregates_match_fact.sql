-- Every aggregate must add up to fct_transactions. Returns the aggregates whose totals
-- differ from the fact's.

with fact as (

    select
        count(*) as transactions,
        sum(quantity) as units,
        sum(net_amount) as net_amount,
        sum(gross_amount) as gross_amount
    from {{ ref('fct_transactions') }}

),

aggregates as (

    select 'agg_daily_sales' as model, sum(transactions) as transactions, sum(units) as units,
           sum(net_amount) as net_amount, sum(gross_amount) as gross_amount
    from {{ ref('agg_daily_sales') }}
    union all
    select 'agg_product_sales', sum(transactions), sum(units), sum(net_amount), sum(gross_amount)
    from {{ ref('agg_product_sales') }}
    union all
    select 'agg_customer_sales', sum(transactions), sum(units), sum(net_amount), sum(gross_amount)
    from {{ ref('agg_customer_sales') }}

)

select
    aggregates.*,
    fact.transactions as fact_transactions,
    fact.units as fact_units,
    fact.net_amount as fact_net_amount,
    fact.gross_amount as fact_gross_amount
from aggregates
cross join fact
where (aggregates.transactions, aggregates.units, aggregates.net_amount, aggregates.gross_amount)
      is distinct from (fact.transactions, fact.units, fact.net_amount, fact.gross_amount)
