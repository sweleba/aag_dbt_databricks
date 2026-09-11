-- FACT_SALES
-- Grain: one row per sales transaction (transaction_id).
-- Surrogate keys: customer_key, product_key, date_key, channel_key - all
-- resolved via the conformed dimensions, never raw source IDs.
-- Late-arriving handling: customer_key always resolves to a real dimension
-- row, even if that row is an inferred stub (dim_customer.is_inferred) -
-- see macros/resolve_late_arriving_customers.sql. product_key is not given
-- the same treatment: sales_transactions.product_id is expected to always
-- reference PIM, the richest and first-processed product source, so an
-- unresolved product_key here signals a genuine data issue worth
-- investigating, not an expected late-arrival - it's left null on purpose
-- so it surfaces in the relationship test rather than being silently
-- papered over.
-- depends_on: {{ ref('_identity_resolution') }}

with txn as (
    select
        transaction_id,
        case when trim(customer_email) = '' then null else lower(trim(customer_email)) end as email_norm,
        {{ normalize_msisdn('customer_msisdn') }} as msisdn_e164,
        product_id,
        cast(quantity as int) as quantity,
        cast(unit_price as decimal(10,2)) as unit_price,
        cast(quantity as int) * cast(unit_price as decimal(10,2)) as amount,
        transaction_date,
        sales_channel
    from {{ source('sales_info', 'sales_transactions') }}
),

resolved_customer as (
    select
        t.transaction_id,
        coalesce(m_email.master_customer_id, m_phone.master_customer_id) as customer_key
    from txn t
    left join {{ var('catalog') }}.{{ var('silver_schema') }}.customer_master m_email
        on t.email_norm is not null and t.email_norm = m_email.email
    left join {{ var('catalog') }}.{{ var('silver_schema') }}.customer_master m_phone
        on t.msisdn_e164 is not null and t.msisdn_e164 = m_phone.msisdn_e164
),

resolved_product as (
    select
        t.transaction_id,
        px.master_product_id as product_key
    from txn t
    left join {{ var('catalog') }}.{{ var('silver_schema') }}.product_xref px
        on px.source_system = 'PIM'
       and px.source_code = t.product_id
       and px.is_active = true
)

select
    t.transaction_id,
    rc.customer_key,
    rp.product_key,
    cast(date_format(t.transaction_date, 'yyyyMMdd') as int) as date_key,
    ch.channel_key,
    t.sales_channel as channel_name,   -- kept alongside the key for readability; drop if you prefer a pure star
    t.quantity,
    t.unit_price,
    t.amount
from txn t
left join resolved_customer rc on t.transaction_id = rc.transaction_id
left join resolved_product rp on t.transaction_id = rp.transaction_id
left join {{ ref('dim_channel') }} ch on t.sales_channel = ch.channel_name