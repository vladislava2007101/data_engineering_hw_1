with bounds as (
    select
        min(order_date) as min_date,
        max(order_date) as max_date
    from {{ ref('stg_shop__orders') }}
),

days as (
    select generate_series(min_date, max_date, interval '1 day')::date as date_day
    from bounds
)

select
    date_day,
    extract(year    from date_day)::int     as year,
    extract(quarter from date_day)::int     as quarter,
    extract(month   from date_day)::int     as month,
    to_char(date_day, 'YYYY-MM')            as year_month,
    extract(isodow  from date_day)::int     as day_of_week,
    extract(isodow  from date_day) in (6, 7) as is_weekend
from days
