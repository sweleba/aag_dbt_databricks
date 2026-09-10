select
    pos_item_code       as source_code,
    'POS'                as source_system,
    item_name            as product_name,
    lower(trim(regexp_replace(item_name, '[^a-zA-Z0-9]', ''))) as product_name_norm,
    dept                 as product_family,
    retail_price          as price,
    cast(null as string)  as status,
    cast(null as date)   as source_updated_at
from {{ source('product_info', 'pos_product_lookup') }}