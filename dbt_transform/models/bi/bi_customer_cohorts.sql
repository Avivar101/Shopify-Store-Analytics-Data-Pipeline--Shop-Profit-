with first_orders as (
    select
        shop_domain,
        customer_id,
        min(order_created_at) as first_order_at
    from {{ ref('fct_customer_orders') }}
    where customer_id is not null
    group by 1,2
)

select
    shop_domain,
    customer_id,
    first_order_at,
    date_trunc('month', first_order_at) as cohort_month
from first_orders