with lines as (
    select *
    from {{ ref('stg_shopify_order_lines') }}
),

orders as (
    select
        shop_domain,
        order_id,
        customer_id,
        created_at as order_created_at,
        financial_status,
        fulfillment_status,
        currency
    from {{ ref('stg_shopify_orders') }}
),

final as (
    select
        l.shop_domain,
        l.order_id,
        l.order_line_id,

        o.customer_id,
        o.order_created_at,
        o.financial_status,
        o.fulfillment_status,
        o.currency,

        l.product_id,
        l.variant_id,
        l.sku,
        l.line_title,

        l.quantity,
        l.unit_price,
        l.line_discount,
        l.line_gross_sales
    from lines l
    left join orders o
        on l.shop_domain = o.shop_domain
        and l.order_id = o.order_id
)

select * from final