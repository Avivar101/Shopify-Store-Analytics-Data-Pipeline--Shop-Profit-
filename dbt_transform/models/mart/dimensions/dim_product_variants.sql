select
    md5(
        coalesce(shop_domain, '') || '|' ||
        cast(variant_id as varchar)
    ) as product_variant_sk,
    shop_domain,
    product_id,
    variant_id,
    variant_title,
    sku,
    price
from {{ ref('stg_shopify_variants') }}