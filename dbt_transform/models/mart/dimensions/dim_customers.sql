select
    shop_domain,
    customer_id,
    created_at,
    email,
    first_name,
    last_name,
    state
from {{ ref('stg_shopify_customers') }}