select
    shop_domain,
    order_id,
    customer_id,
    created_at,
    updated_at,
    order_number,
    order_name,
    currency,
    financial_status,
    subtotal_price,
    total_discounts,
    total_tax,
    total_price
from {{ ref('stg_shopify_orders') }}