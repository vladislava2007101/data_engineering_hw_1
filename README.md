# store_hse

## Хранение в Greenplum

| Витрина | Распределение | Почему |
|---|---|---|
| fct_orders | `distributed by (order_id)` | Уникальный ключ — строки ровно ложатся по сегментам. Совпадает с ключом raw.orders/payments и fct_order_items, поэтому JOIN между фактами идёт без пересылки строк. |
| fct_order_items | `distributed by (order_id)`, колоночное | Тот же ключ, что у fct_orders: позиции заказа лежат на одном сегменте с заказом. Если распределить по order_item_id, каждый JOIN с fct_orders требовал бы redistribute motion. Колоночное хранение — отчёты читают 2–3 колонки широкого факта. |
| dim_customers | `distributed replicated` | 40 строк. Факты распределены по order_id, а соединяются с клиентами по customer_id — копия на каждом сегменте убирает пересылку. |
| dim_products | `distributed replicated` | 13 строк, соединяется с fct_order_items по product_id — тот же довод. |
| dim_dates | `distributed replicated` | 133 строки, соединяется с фактами по дате — тот же довод. |
