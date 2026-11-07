-- Сумма заказа в fct_orders равна сумме line_amount его позиций в fct_order_items.
-- Возвращает заказы, где суммы не совпали.
with items as (
    select
        order_id,
        sum(line_amount) as items_amount
    from {{ ref('fct_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.order_amount,
    coalesce(i.items_amount, 0) as items_amount
from {{ ref('fct_orders') }} as o
left join items as i
    on i.order_id = o.order_id
where o.order_amount <> coalesce(i.items_amount, 0)
