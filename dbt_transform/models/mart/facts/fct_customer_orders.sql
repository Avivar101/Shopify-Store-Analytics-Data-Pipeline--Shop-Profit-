select
    shop_domain,
    customer_id,
    order_id,
    created_at as order_created_at,
    total_price,
    total_discounts,
    total_tax
from {{ ref('stg_shopify_orders') }}
where customer_id is not null