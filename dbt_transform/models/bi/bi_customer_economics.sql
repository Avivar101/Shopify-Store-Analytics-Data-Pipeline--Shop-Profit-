with customer_order_stats as (
    select
        shop_domain,
        customer_id,
        count(distinct order_id) as total_orders,
        min(order_created_at) as first_order_at,
        max(order_created_at) as last_order_at,
        sum(total_price) as lifetime_value
    
    from {{ ref('fct_customer_orders') }}
    where customer_id is not null
    group by 1, 2
),

final as (
    select
        shop_domain,
        customer_id,
        total_orders,
        first_order_at,
        last_order_at,
        lifetime_value,

        case
            when total_orders > 1 then true
            else false
        end as is_repeat_customer
    
    from customer_order_stats
)

select * from final