select
    sku                as source_code,
    'ERP'               as source_system,
    description         as product_name,
    lower(trim(regexp_replace(description, '[^a-zA-Z0-9]', ''))) as product_name_norm,
    product_family,
    unit_cost           as price,
    cast(null as string) as status,
    cast(null as date)  as source_updated_at
from {{ source('product_info', 'erp_product_catalogue') }}