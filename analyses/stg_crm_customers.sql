-- Silver: standardized CRM customer records.
-- Addresses: split first/last name -> single normalized name; inconsistent
-- casing; blank-vs-null national_id and email.

select
    crm_customer_id                                            as source_id,
    'CRM'                                                       as source_system,
    concat_ws(' ', initcap(trim(first_name)), initcap(trim(last_name))) as full_name,
    lower(trim(concat_ws(' ', first_name, last_name)))          as full_name_norm,
    case when trim(email) = '' then null else lower(trim(email)) end as email_norm,
    nullif(trim(national_id), '')                               as national_id,
    date_of_birth,
    case when trim(city) = '' then null else initcap(trim(city)) end as city_norm,
    status,
    to_timestamp(created_date)                                  as source_updated_at,
    nullif(trim(last_purchased_product_id), '')                 as last_purchased_code,
    'PIM'                                                        as last_purchased_code_type,
    null                                                         as outstanding_balance,
    null                                                         as currency,
    null                                                         as marketing_opt_in
from {{ source('customer_info', 'crm_customers') }}