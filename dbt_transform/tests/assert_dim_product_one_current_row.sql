select
    shop_domain,
    product_id,
    count(*) as current_row_count
from {{ ref('dim_products') }}
where is_current = true
group by 1, 2
having count(*) > 1