select *
from {{ ref('fct_orders') }}
where updated_at < created_at