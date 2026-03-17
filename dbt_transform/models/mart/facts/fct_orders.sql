{{
  config(
    materialized = 'incremental',
    unique_key = 'order_id',
    incremental_strategy='merge'
    )
}}

{% set transform_batch_no = var('transform_batch_no') %}

with changed_orders as (
    select
        order_id,
        updated_at as order_updated_at
    from {{ ref('stg_shopify_orders') }}

    {% if is_incremental() %}
      where updated_at >= coalesce((
        select max(order_updated_at) - interval '1 day' 
        from {{ this }}), '1900-01-01')
    {% endif %}
),

orders as (
    select
        shop_domain,
        order_id,
        customer_id,
        created_at as order_created_at,
        updated_at as order_updated_at,
        order_number,
        order_name,
        financial_status,
        fulfillment_status,
        subtotal_price,
        total_discounts,
        total_tax,
        total_price,
        currency
    from {{ ref('stg_shopify_orders') }}
    where order_id in (select order_id from changed_orders)
)

select
    shop_domain,
    order_id,
    customer_id,
    order_created_at,
    order_updated_at,
    order_number,
    order_name,
    currency,
    financial_status,
    subtotal_price,
    total_discounts,
    total_tax,
    total_price,

    '{{ transform_batch_no }}' as transform_batch_no
from orders