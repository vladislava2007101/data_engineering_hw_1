{{ config(distributed_by='order_id') }}

with items as (
    select * from {{ ref('stg_shop__order_items') }}
),

orders as (
    select * from {{ ref('int_orders_with_payments') }}
)

select
    i.order_item_id,
    i.order_id,
    i.product_id,
    o.customer_id,
    o.order_date,
    o.order_status,
    i.quantity,
    i.unit_price,
    i.gross_amount,
    i.discount_amount,
    i.line_amount,
    o.is_confirmed
from items as i
join orders as o
    on o.order_id = i.order_id
where not o.is_test_order
