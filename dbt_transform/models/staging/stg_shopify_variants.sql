{{
  config(
    materialized = 'view',
    )
}}

with products as (
    select
        shop_domain,
        payload_json,
        ingested_at,
    from {{ ref('stg_shopify_products') }}
),

variants as (
    select
        p.shop_domain,
        try_cast(json_extract(p.payload_json, '$.id') as bigint) as product_id,
        v.value as variant_json,
        p.ingested_at
    from products p
    cross join json_each(json_extract(p.payload_json, '$.variants')) v
)

select
    shop_domain,
    product_id,
    try_cast(json_extract(variant_json, '$.id') as bigint) as variant_id,

    json_extract_string(variant_json, '$.title') as variant_title,
    json_extract_string(variant_json, '$.sku') as sku,
    try_cast(json_extract_string(variant_json, '$.price') as double) as price,
    try_cast(json_extract(variant_json, '$.position') as integer) as position,

    ingested_at
from variants