{{
  config(
    materialized = 'view',
    )
}}

with raw as (
    select
        shop_domain,
        payload_json,
        ingested_at
    from {{ source('raw', 'raw_shopify_customers') }}
),

dedup as (
    select
        *,
        row_number() over (
            partition by shop_domain, try_cast(json_extract(payload_json, '$.id') as bigint)
            order by
                try_cast(json_extract_string(payload_json, '$.updated_at') as timestamptz) desc nulls last,
                ingested_at desc

        ) as rn
    from raw
)

select
    shop_domain,
    try_cast(json_extract_string(payload_json, '$.id') as bigint) as customer_id,

    try_cast(json_extract_string(payload_json, '$.created_at') as timestamptz) as created_at,
    try_cast(json_extract_string(payload_json, '$.updated_at') as timestamptz) as updated_at,

    lower(json_extract_string(payload_json, '$.email')) as email,
    json_extract_string(payload_json, '$.first_name') as first_name,
    json_extract_string(payload_json, '$.last_name') as last_name,
    json_extract_string(payload_json, '$.state') as state,

    try_cast(json_extract_string(payload_json, '$.orders_count') as integer) as orders_count,
    try_cast(json_extract_string(payload_json, '$.total_spent') as double) as total_spent,

    ingested_at
from dedup
where rn = 1