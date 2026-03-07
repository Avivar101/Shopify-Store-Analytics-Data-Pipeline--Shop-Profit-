{{ config(materialized='view') }}

with raw as (
    select
        shop_domain,
        payload_json,
        ingested_at
    from {{ source('raw', 'raw_shopify_products') }}
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
    try_cast(json_extract(payload_json, '$.id') as bigint) as product_id,

    try_cast(json_extract_string(payload_json, '$.created_at') as timestamptz) as created_at,
    try_cast(json_extract_string(payload_json, '$.updated_at') as timestamptz) as updated_at,

    json_extract_string(payload_json, '$.title') as product_title,
    json_extract_string(payload_json, '$.vendor') as vendor,
    json_extract_string(payload_json, '$.product_type') as product_type,
    json_extract_string(payload_json, '$.status') as status,

    payload_json,
    ingested_at
from dedup
where rn = 1