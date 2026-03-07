{{
  config(
    materialized = 'view',
    )
}}

with raw as (
    select
        shop_domain,
        order_id,
        updated_at as raw_updated_at,
        ingested_at,
        payload_json
    from {{ source('raw', 'raw_shopify_orders') }}
),

dedup as (
    select
        *,
        row_number() over (
            partition by shop_domain, order_id
            order by
                try_cast(json_extract_string(payload_json, '$.updated_at') as timestamptz) desc nulls last,
                ingested_at desc
            
        )   as rn
    from raw
),

final as (
    select
        shop_domain,
        order_id,

        -- identifiers
        json_extract_string(payload_json, '$.name') as order_name,
        try_cast(json_extract(payload_json, '$.order_number') as bigint) as order_number,
        try_cast(json_extract(payload_json, '$.customer.id') as bigint) as customer_id,

        -- timestamps
        try_cast(json_extract_string(payload_json, '$.created_at') as timestamptz) as created_at,
        try_cast(json_extract_string(payload_json, '$.processed_at') as timestamptz) as processed_at,
        try_cast(json_extract_string(payload_json, '$.updated_at') as timestamptz) as updated_at,
        try_cast(json_extract_string(payload_json, '$.cancelled_at') as timestamptz) as cancelled_at,

        -- statuses
        json_extract_string(payload_json, '$.financial_status') as financial_status,
        json_extract_string(payload_json, '$.fulfillment_status') as fulfillment_status,
        
        -- money/currency
        json_extract_string(payload_json, '$.currency') as currency,
        try_cast(json_extract(payload_json, '$.subtotal_price') as double) as subtotal_price,
        try_cast(json_extract(payload_json, '$.total_tax') as double) as total_tax,
        try_cast(json_extract(payload_json, '$.total_discounts') as double) as total_discounts,
        try_cast(json_extract(payload_json, '$.total_price') as double) as total_price,

        --raw lineage
        ingested_at,
        payload_json
    from dedup
    where rn = 1
)

select * from final