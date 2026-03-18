{% snapshot dim_product_snapshot %}

{{
   config(
       target_schema='snapshots',
       unique_key='product_business_key',
       strategy='check',
       check_cols=['product_title', 'vendor', 'product_type', 'status']
   )
}}

select
    cast(shop_domain as varchar) || '-' || cast(product_id as varchar) as product_business_key,
    shop_domain,
    product_id,
    product_title,
    vendor,
    product_type,
    status,
    updated_at
from {{ ref('stg_shopify_products') }}


{% endsnapshot %}