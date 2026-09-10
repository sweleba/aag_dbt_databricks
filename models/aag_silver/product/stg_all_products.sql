-- Silver: standardized product records from all three catalogues.
-- Product names are near-identical across sources (just differently cased),
-- so a single normalized-name model covers all three - no per-source split
-- needed the way customer data required.

select
    source_code,
    source_system,
    product_name,
    product_name_norm,
    product_family,
    price,
    status,
    source_updated_at
from {{ ref('stg_pim_product') }}

union all

select
    source_code,
    source_system,
    product_name,
    product_name_norm,
    product_family,
    price,
    status,
    source_updated_at
from {{ ref('stg_erp_product') }}

union all

select
    source_code,
    source_system,
    product_name,
    product_name_norm,
    product_family,
    price,
    status,
    source_updated_at
from {{ ref('stg_pos_product') }}