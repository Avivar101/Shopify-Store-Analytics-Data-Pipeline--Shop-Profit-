{% snapshot dim_product_snapshot %}

{{
   config(
       target_database='target_database',
       target_schema='snapshots',
       unique_key='shop_domain || ''-'' || cast(product_id as varchar)',
       strategy='check',
       check_cols=['product_title', 'vendor', 'product_type', 'status']
   )
}}

select
    shop_domain,
    product_id,
    product_title,
    vendor,
    product_type,
    status,
    updated_at
from {{ ref('stg_shopify_products') }}


{% endsnapshot %}