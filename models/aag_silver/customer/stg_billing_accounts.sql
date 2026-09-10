select
    billing_account_id                                          as source_id,
    'BILLING'                                                    as source_system,
    initcap(trim(full_name))                                    as full_name,
    lower(trim(full_name))                                      as full_name_norm,
    case when trim(email_address) = '' then null else lower(trim(email_address)) end as email_norm,
    {{ normalize_msisdn('mobile_number') }}                     as msisdn_e164,
    nullif(trim(id_number), '')                                 as national_id,
    cast(null as date)                                          as date_of_birth,  -- not captured by Billing
    cast(null as string)                                        as city_norm,       -- not captured by Billing
    account_type                                                as status,
    to_timestamp(last_invoice_date)                             as source_updated_at,
    nullif(trim(last_purchased_sku), '')                        as last_purchased_code,
    'ERP'                                                       as last_purchased_code_type,
    outstanding_balance,
    currency,
    null                                                         as marketing_opt_in
from {{ source('customer_info', 'billing_accounts') }}