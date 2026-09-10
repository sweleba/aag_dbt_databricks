-- Conformed customer dimension, built from the Silver golden record.
-- Currently Type 1 (always reflects latest survivorship result) in the
-- normal dbt build. Full history is captured separately via the
-- snapshots/dim_customer_snapshot.sql dbt snapshot (Type 2) - query that
-- instead of this model wherever historical point-in-time reporting matters.
-- depends_on: {{ ref('_identity_resolution') }}

select
    master_customer_id as customer_key,
    full_name,
    full_name_source,
    email,
    email_source,
    msisdn_e164,
    msisdn_source,
    national_id,
    national_id_source,
    date_of_birth,
    city_norm            as city,
    outstanding_balance,
    currency,
    marketing_opt_in,
    match_confidence,
    created_at            as customer_since,
    updated_at             as last_updated_at
from {{ var('catalog') }}.{{ var('silver_schema') }}.customer_master