{{
  config(
    materialized = 'table',
    )
}}

select
    shop_domain,
    date_trunc('day', order_created_at) as order_date,

    sum(line_gross_sales) as total_sales,
    count(distinct order_id) as total_orders,
    sum(quantity) as units_sold,

    case
        when count(distinct order_id) = 0 then 0
        else sum(line_gross_sales) / count(distinct order_id)
    end as average_order_value

from {{ ref('fct_order_items') }}

group by 1, 2