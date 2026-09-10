{#
  Kimball's standard technique for a late-arriving dimension member: when a
  fact (sales transaction, inquiry, email, WhatsApp message) references a
  customer via email/msisdn that doesn't match any existing customer_master
  row, don't leave the fact's customer_key null - insert a minimal
  "inferred" stub row now (is_inferred = true) so the fact has a real key
  to join to. If that person later shows up in CRM/Billing/Ecommerce,
  resolve_customer_identity's normal matching (email/msisdn tiers) will
  find this stub row like any other master record and enrich it via
  survivorship - the row's identity persists, it just gains attributes.

  Must run AFTER resolve_customer_identity() (so real source-driven
  matching gets first pick) and BEFORE the Gold fact models build.
#}
{% macro resolve_late_arriving_customers() %}

  {% set catalog = var('catalog') %}
  {% set schema = var('silver_schema') %}
  {% set bronze = var('bronze_schema') %}
  {% set qualify = catalog ~ "." ~ schema %}

  {% set fact_identities %}
    CREATE OR REPLACE TEMPORARY VIEW fact_identities_unmatched AS
    WITH fact_ids AS (
      SELECT
        CASE WHEN trim(customer_email) = '' THEN NULL ELSE lower(trim(customer_email)) END AS email_norm,
        {{ normalize_msisdn('customer_msisdn') }} AS msisdn_e164
      FROM {{ source('sales_info', 'sales_transactions') }}

      UNION ALL
      SELECT
        CASE WHEN trim(email) = '' THEN NULL ELSE lower(trim(email)) END,
        {{ normalize_msisdn('msisdn') }}
      FROM {{ source('interactions_info', 'customer_inquiries') }}

      UNION ALL
      SELECT
        CASE WHEN trim(from_email) = '' THEN NULL ELSE lower(trim(from_email)) END,
        NULL
      FROM {{ source('interactions_info', 'email_interactions') }}

      UNION ALL
      SELECT
        NULL,
        {{ normalize_msisdn('customer_msisdn') }}
      FROM {{ source('interactions_info', 'whatsapp_interactions') }}
    ),
    distinct_ids AS (
      SELECT DISTINCT email_norm, msisdn_e164
      FROM fact_ids
      WHERE email_norm IS NOT NULL OR msisdn_e164 IS NOT NULL
    )
    SELECT d.email_norm, d.msisdn_e164
    FROM distinct_ids d
    LEFT JOIN {{ qualify }}.customer_master m
      ON (d.email_norm IS NOT NULL AND d.email_norm = m.email)
      OR (d.msisdn_e164 IS NOT NULL AND d.msisdn_e164 = m.msisdn_e164)
    WHERE m.master_customer_id IS NULL
  {% endset %}
  {% do run_query(fact_identities) %}

  {% set insert_stubs %}
    INSERT INTO {{ qualify }}.customer_master (
      master_customer_id, email, email_source, msisdn_e164, msisdn_source,
      match_confidence, is_inferred, created_at, updated_at
    )
    SELECT
      concat('CUST-INFERRED-', replace(uuid(), '-', '')),
      email_norm, CASE WHEN email_norm IS NOT NULL THEN 'FACT_INFERRED' END,
      msisdn_e164, CASE WHEN msisdn_e164 IS NOT NULL THEN 'FACT_INFERRED' END,
      0.0, true, current_timestamp(), current_timestamp()
    FROM fact_identities_unmatched
  {% endset %}
  {% do run_query(insert_stubs) %}

  {{ log("Late-arriving customer stubs created for any fact identities with no existing master.", info=true) }}

{% endmacro %}
