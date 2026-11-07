-- Сверка с источником: сумма подтверждённых покупок в витрине
with source_revenue as (
    select sum(o.total_amount) as revenue
    from {{ source('shop', 'orders') }} as o
    where o.status = 'paid'
      and exists (
          select 1
          from {{ source('shop', 'payments') }} as p
          where p.order_id = o.order_id
            and p.payment_status = 'success'
            and p.amount > 0
      )
),

mart_revenue as (
    select sum(order_amount) as revenue
    from {{ ref('fct_orders') }}
    where is_confirmed
)

select
    s.revenue as source_revenue,
    m.revenue as mart_revenue
from source_revenue as s
cross join mart_revenue as m
where s.revenue <> m.revenue
