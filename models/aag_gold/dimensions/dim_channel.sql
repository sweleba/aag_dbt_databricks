-- Conformed channel dimension. Values are collected from every source that
-- carries a channel-like field (sales, ecommerce signup, interactions) so
-- one dimension covers all of them, rather than each fact inventing its
-- own channel list.
-- Grain: one row per distinct channel name.
-- Surrogate key: channel_key, a dense integer, independent of any source
-- system's own representation of "channel".
-- depends_on: {{ ref('_identity_resolution') }}

with raw_channels as (
    select distinct sales_channel as channel_name from {{ source('sales_info', 'sales_transactions') }}
    union
    select distinct signup_channel from {{ source('customer_info', 'ecommerce_users') }}
    union
    select distinct channel from {{ source('interactions_info', 'customer_inquiries') }}
    union
    select 'Email' as channel_name
    union
    select 'WhatsApp' as channel_name
)

select
    row_number() over (order by channel_name)  as channel_key,
    channel_name
from raw_channels
where channel_name is not null and trim(channel_name) != ''
