select
    web_user_id                                                 as source_id,
    'ECOMMERCE'                                                  as source_system,
    initcap(trim(display_name))                                 as full_name,
    lower(trim(display_name))                                   as full_name_norm,
    case when trim(login_email) = '' then null else lower(trim(login_email)) end as email_norm,
    {{ normalize_msisdn('contact_number') }}                    as msisdn_e164,
    cast(null as string)                                        as national_id,   -- not captured by Ecommerce
    cast(null as date)                                          as date_of_birth,  -- not captured by Ecommerce
    case when trim(city) = '' then null else initcap(trim(city)) end as city_norm,
    signup_channel                                              as status,
    to_timestamp(last_login_date)                               as source_updated_at,
    nullif(trim(last_purchased_item_code), '')                  as last_purchased_code,
    'POS'                                                        as last_purchased_code_type,
    null                                                         as outstanding_balance,
    null                                                         as currency,
    cast(marketing_opt_in as boolean)                           as marketing_opt_in
from {{ source('customer_info', 'ecommerce_users') }}