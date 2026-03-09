{{ config(materialized='table') }}

with customer_first_order as (

    select
        shop_domain,
        customer_id,
        min(order_created_at) as first_order_at
    from {{ ref('fct_customer_orders') }}
    where customer_id is not null
    group by 1, 2

),

orders_labeled as (

    select
        o.shop_domain,
        o.order_id,
        o.customer_id,
        o.order_created_at,
        o.total_price,
        c.first_order_at,

        case
            when o.order_created_at = c.first_order_at then 'new'
            else 'returning'
        end as customer_type

    from {{ ref('fct_customer_orders') }} o
    join customer_first_order c
      on o.shop_domain = c.shop_domain
     and o.customer_id = c.customer_id
)

select
    shop_domain,
    date_trunc('day', order_created_at) as order_date,
    customer_type,
    sum(total_price) as total_sales,
    count(distinct order_id) as total_orders
from orders_labeled
group by 1, 2, 3