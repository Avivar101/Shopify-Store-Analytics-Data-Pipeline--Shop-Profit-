select
    shop_domain,

    count(distinct customer_id) as total_customers,

    sum(case when is_repeat_customer then 1 else 0 end) as repeat_customer,

    case
        when count(distinct customer_id) = 0 then 0
        else sum(case when is_repeat_customer then 1 else 0 end) * 1.0
            / count(distinct customer_id)
    end as repeat_purchase_rate,

    avg(lifetime_value) as average_customer_ltv

from {{ ref('bi_customer_economics') }}

group by 1