{{
  config(
    materialized='table',
    post_hook=[
      "{{ resolve_customer_identity() }}",
      "{{ resolve_product_identity() }}",
      "{{ resolve_late_arriving_customers() }}"
    ]
  )
}}

-- Exists purely to sequence the dbt DAG correctly: refs all four
-- standardized staging models (forcing them to build first), then its
-- post_hook runs, in order: customer identity resolution, product identity
-- resolution, and finally late-arriving customer stub creation (for facts
-- whose email/phone matches nobody CRM/Billing/Ecommerce has ever sent).
-- Every Gold model refs THIS model (not the staging models directly) so
-- dbt's dependency graph forces Gold to wait until all of this has
-- actually completed for this run - see the ordering note in
-- dbt_project.yml for why this can't just be an on-run-end hook.

select
    (select count(*) from {{ ref('stg_crm_customers') }})    as crm_rows,
    (select count(*) from {{ ref('stg_billing_accounts') }}) as billing_rows,
    (select count(*) from {{ ref('stg_ecommerce_users') }})  as ecommerce_rows,
    (select count(*) from {{ ref('stg_all_products') }})   as product_rows,
    current_timestamp()                                       as resolved_at