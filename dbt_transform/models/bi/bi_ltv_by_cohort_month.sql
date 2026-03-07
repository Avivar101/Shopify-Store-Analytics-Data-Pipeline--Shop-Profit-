with customer_orders as (
    select
        o.shop_domain,
        o.customer_id,
        o.order_created_at,
        o.total_price,
        c.cohort_month
    from {{ ref('fct_customer_orders') }} o

    join {{ ref('bi_customer_cohorts') }} c
        on o.customer_id = c.customer_id
        and o.shop_domain = c.shop_domain
),

cohort_revenue as (
    select
        shop_domain,
        cohort_month,

        date_diff(
            'month',
            cohort_month,
            date_trunc('month', order_created_at)
        ) as months_since_acquisition,

        sum(total_price) as cohort_revenue
    from customer_orders
    group by 1,2,3
)

select * from cohort_revenue