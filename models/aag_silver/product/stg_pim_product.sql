select
    product_id        as source_code,
    'PIM'              as source_system,
    product_name       as product_name,
    lower(trim(regexp_replace(product_name, '[^a-zA-Z0-9]', ''))) as product_name_norm,
    category           as product_family,
    list_price         as price,
    status,
    launch_date        as source_updated_at
from {{ source('product_info', 'pim_product_master') }}