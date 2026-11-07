-- Возвращает возвращённые заказы, где остались деньги клиента или вернули больше.
select
    order_id,
    paid_amount,
    refunded_amount,
    net_paid_amount
from {{ ref('fct_orders') }}
where order_status = 'returned'
  and net_paid_amount <> 0
