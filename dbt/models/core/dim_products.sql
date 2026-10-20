-- Products with the history of their names (SCD type 2): one row per version, valid
-- from valid_from (inclusive) to valid_to (exclusive). Derived from the transactions,
-- since there is no product master source: a product's name on a day is the one its
-- transactions show, and a new name starts a new version on that day.
--
-- Incremental, and history is never rewritten: each run takes the new loads'
-- transactions dated after a product's last_seen_on and applies them in date order
-- to its current version. Earlier transactions whose name disagrees with the version
-- covering their date are not applied: tests/core/assert_product_names_match_dim.sql
-- warns about them, and a full refresh of dim_products+ applies them.

{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='product_key',
        on_schema_change='fail',
    )
}}

with observations as (

    select product_id, product_name, transaction_date, _load_id, _source_row
    from {{ ref('stg_customer_transactions') }}
    where product_id is not null
        and product_name is not null
        and transaction_date is not null
        {% if is_incremental() %}
            and _load_id > {{ load_watermark() }}
        {% endif %}

),

-- One name per product and day: the last one delivered if they differ.
daily_names as (

    select distinct on (product_id, transaction_date)
        product_id,
        transaction_date as seen_on,
        product_name
    from observations
    order by product_id asc, transaction_date asc, _load_id desc, _source_row desc

),

product_loads as (

    select product_id, max(_load_id) as _load_id
    from observations
    group by product_id

),

-- The names to version, in date order. On incremental runs, each product's current
-- version comes first, then the newer days; a full refresh versions every day.
timeline as (

    {% if is_incremental() %}

        select product_id, last_seen_on as seen_on, product_name, valid_from as current_valid_from
        from {{ this }}
        where is_current
            and product_id in (select product_loads.product_id from product_loads)

        union all

        select daily_names.product_id, daily_names.seen_on, daily_names.product_name, null::date
        from daily_names
        left join {{ this }} as current_version
            on daily_names.product_id = current_version.product_id
                and current_version.is_current
        where current_version.product_id is null
            or daily_names.seen_on > current_version.last_seen_on

    {% else %}

    select product_id, seen_on, product_name, null::date as current_valid_from
    from daily_names

{% endif %}

),

changes as (

    select
        *,
        lag(product_name) over (partition by product_id order by seen_on) as previous_name
    from timeline

),

-- A version starts on each day whose name differs from the previous day's.
numbered as (

    select
        *,
        sum(case when product_name is distinct from previous_name then 1 else 0 end)
            over (partition by product_id order by seen_on)
        as version_number
    from changes

),

versions as (

    select
        product_id,
        min(product_name) as product_name,
        -- A product's first version is valid from the start of time, so a transaction
        -- dated before its first observed sale still has a version. The current
        -- version keeps its start, which its key is built from.
        coalesce(
            min(current_valid_from),
            case when version_number = 1 then date '1900-01-01' else min(seen_on) end
        ) as valid_from,
        max(seen_on) as last_seen_on
    from numbered
    group by product_id, version_number

),

versioned as (

    select
        *,
        coalesce(
            lead(valid_from) over (partition by product_id order by valid_from),
            date '9999-12-31'
        ) as valid_to
    from versions

)

select
    {{ dbt_utils.generate_surrogate_key(['versioned.product_id', 'versioned.valid_from']) }} as product_key,
    versioned.product_id,
    versioned.product_name,
    versioned.valid_from,
    versioned.valid_to,
    versioned.valid_to = date '9999-12-31' as is_current,
    versioned.last_seen_on,
    product_loads._load_id
from versioned
inner join product_loads using (product_id)
