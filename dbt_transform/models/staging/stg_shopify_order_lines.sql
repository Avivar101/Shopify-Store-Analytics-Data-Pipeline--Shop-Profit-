{{
  config(
    materialized = 'view',
    )
}}

with orders as (
    -- reuse the already-deduped staging model so you don't reimplement dedup logic
    select
        shop_domain,
        order_id,
        created_at as order_created_at,
        updated_at as order_updated_at,
        currency,
        payload_json
    from {{ ref('stg_shopify_orders') }}
),

line_items as (
    select
        o.shop_domain,
        o.order_id,
        o.order_created_at,
        o.order_updated_at,
        o.currency,

        -- json_each returns (key, value); value is each line_iten JSON object
        li.value as line_item_json
    from orders o
    cross join json_each(json_extract(o.payload_json, '$.line_items')) li
),

final as (
    select
        shop_domain,
        order_id,
        order_created_at,
        order_updated_at,
        currency,

        try_cast(json_extract(line_item_json, '$.id') as bigint) as order_line_id,
        try_cast(json_extract(line_item_json, '$.product_id') as bigint) as product_id,
        try_cast(json_extract(line_item_json, '$.variant_id') as bigint) as variant_id,

        json_extract_string(line_item_json, '$.title') as line_title,
        json_extract_string(line_item_json, '$.sku') as sku,

        try_cast(json_extract(line_item_json, '$.quantity') as integer) as quantity,
        try_cast(json_extract_string(line_item_json, '$.price') as double) as unit_price,
        try_cast(json_extract_string(line_item_json, '$.total_discount') as double) as line_discount,

        -- line gross (simple, pre-tax, pre-shipping)
        try_cast(json_extract(line_item_json, '$.quantity') as integer) * 
            try_cast(json_extract_string(line_item_json, '$.price') as double) as line_gross_sales
    from line_items
)

select * from final