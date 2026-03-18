select
    f.shop_domain,
    f.product_id,
    f.variant_id,

    d.variant_title,
    d.sku,

    sum(f.line_gross_sales) as total_sales,
    sum(f.quantity) as units_sold,
    count(distinct f.order_id) as total_orders,

    case
        when count(distinct f.order_id) = 0 then 0
        else sum(f.line_gross_sales) / count(distinct f.order_id)
    end as average_sales_per_order

from {{ ref('fct_order_items') }} f
left join {{ ref('dim_product_variants') }} d
    on f.shop_domain = d.shop_domain
    and f.variant_id = d.variant_id

group by
    f.shop_domain,
    f.product_id,
    f.variant_id,
    d.variant_title,
    d.sku