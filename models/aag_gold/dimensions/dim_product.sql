-- Conformed product dimension, built from the Silver golden record.
-- depends_on: {{ ref('_identity_resolution') }}

select
    master_product_id as product_key,
    product_name,
    product_family,
    list_price,
    match_confidence,
    created_at          as product_since,
    updated_at           as last_updated_at
from {{ var('catalog') }}.{{ var('silver_schema') }}.product_master