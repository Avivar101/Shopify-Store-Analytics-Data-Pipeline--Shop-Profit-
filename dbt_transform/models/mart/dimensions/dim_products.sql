{{
  config(
    materialized = 'table',
    )
}}

select
    md5(
        coalesce(shop_domain, '') || '|' ||
        cast(product_id as varchar) || '|' ||
        cast(dbt_valid_from as varchar)
    ) as product_sk,
    shop_domain,
    product_id,
    product_title,
    vendor,
    product_type,
    status,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case
        when dbt_valid_to is null then true
        else false
    end as is_current
from {{ ref('dim_product_snapshot') }}