{#
  Normalizes phone numbers to E.164 (+<country><number>).

  Observed formats in the source data:
    - crm_customers.phone         already "+27..." (has the plus)
    - billing_accounts.mobile_number   digits only, "27..." (country code, no plus)
    - ecommerce_users.contact_number   same as billing, sometimes blank (~26% blank)

  Adjust the digit-length heuristic if your real data includes non-SA numbers.
#}
{% macro normalize_msisdn(column_name) %}
  case
    when {{ column_name }} is null or trim({{ column_name }}) = '' then null
    when trim({{ column_name }}) like '+%'
      then concat('+', regexp_replace({{ column_name }}, '[^0-9]', ''))
    when regexp_replace({{ column_name }}, '[^0-9]', '') like '27%'
         and length(regexp_replace({{ column_name }}, '[^0-9]', '')) >= 11
      then concat('+', regexp_replace({{ column_name }}, '[^0-9]', ''))
    when length(regexp_replace({{ column_name }}, '[^0-9]', '')) = 9
      then concat('+27', regexp_replace({{ column_name }}, '[^0-9]', ''))
    when regexp_replace({{ column_name }}, '[^0-9]', '') = '' then null
    else concat('+', regexp_replace({{ column_name }}, '[^0-9]', ''))
  end
{% endmacro %}