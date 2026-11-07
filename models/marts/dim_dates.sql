{{ config(distributed_replicated=true) }}
with days as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="(select min(order_date) from " ~ ref('stg_shop__orders') ~ ")",
        end_date="(select max(order_date) + 1 from " ~ ref('stg_shop__orders') ~ ")"
    ) }}
)


select
    date_day::date as date_day,
    extract(year    from date_day)::int     as year,
    extract(quarter from date_day)::int     as quarter,
    extract(month   from date_day)::int     as month,
    to_char(date_day, 'YYYY-MM')            as year_month,
    extract(isodow  from date_day)::int     as day_of_week,
    extract(isodow  from date_day) in (6, 7) as is_weekend
from days
