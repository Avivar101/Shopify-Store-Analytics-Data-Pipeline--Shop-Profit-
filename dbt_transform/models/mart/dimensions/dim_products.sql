select
    p.shop_domain,
    p.product_id,
    v.variant_id,
    p.product_title,
    v.variant_title,
    v.sku,
    v.price,
    p.vendor,
    p.product_type,
    p.status
from {{ ref('stg_shopify_products') }} p
left join {{ ref('stg_shopify_variants') }} v
    on p.shop_domain = v.shop_domain
    and p.product_id = v.product_id