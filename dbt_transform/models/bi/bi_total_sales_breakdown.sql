select
    f.shop_domain,
    date_trunc('day', f.order_created_at) as order_date,

    f.product_id,
    f.variant_id,

    d.variant_title,
    d.sku,

    sum(f.line_gross_sales) as total_sales,
    sum(f.quantity) as units_sold,
    count(distinct f.order_id) as total_orders

from {{ ref('fct_order_items') }} f
left join {{ ref('dim_product_variants') }} d
    on f.shop_domain = d.shop_domain
    and f.variant_id = d.variant_id

group by
1, 2, 3, 4, 5, 6