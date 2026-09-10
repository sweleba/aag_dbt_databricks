{#
  Runs the 6 SOP data-quality dimensions and logs every result to
  dq_results, keyed by rule_id + run timestamp, so a scorecard can trend
  pass rates over time and alert on threshold breaches.

  Uses dbt's invocation_id as check_run_id so every check from this run
  shares one identifier - makes it easy to query "show me everything from
  the last run" or diff two runs against each other.

  This is a representative set of checks per dimension, not exhaustive -
  see README for how to add more.
#}
{% macro _log_check(qualify, run_id, rule_id, dimension, entity, severity, tested_expr, failed_expr, details_expr='NULL') %}
  INSERT INTO {{ qualify }}.dq_results (
    check_run_id, rule_id, dimension, entity, severity,
    records_tested, records_failed, pass_rate, run_timestamp, details
  )
  SELECT
    '{{ run_id }}', '{{ rule_id }}', '{{ dimension }}', '{{ entity }}', '{{ severity }}',
    tested, failed,
    CASE WHEN tested = 0 THEN NULL ELSE ROUND(1.0 - (failed / tested), 4) END,
    current_timestamp(),
    {{ details_expr }}
  FROM (
    SELECT CAST({{ tested_expr }} AS DOUBLE) AS tested, CAST({{ failed_expr }} AS DOUBLE) AS failed
  )
{% endmacro %}

{% macro run_customer_dq_checks() %}

  {% set catalog = var('catalog') %}
  {% set schema = var('silver_schema') %}
  {% set qualify = catalog ~ "." ~ schema %}
  {% set run_id = invocation_id %}

  -- ==========================================================================
  -- COMPLETENESS: % of critical fields populated, per source and per golden record
  -- ==========================================================================
  {% set q %}
    {{ _log_check(qualify, run_id,
      'COMPL_CRM_EMAIL', 'Completeness', 'stg_crm_customers', 'WARNING',
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_incoming_batch WHERE source_system='CRM')",
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_incoming_batch WHERE source_system='CRM' AND email_norm IS NULL)"
    ) }}
  {% endset %}
  {% do run_query(q) %}

  {% set q %}
    {{ _log_check(qualify, run_id,
      'COMPL_ECOM_PHONE', 'Completeness', 'stg_ecommerce_users', 'INFO',
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_incoming_batch WHERE source_system='ECOMMERCE')",
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_incoming_batch WHERE source_system='ECOMMERCE' AND msisdn_e164 IS NULL)"
    ) }}
  {% endset %}
  {% do run_query(q) %}

  {% set q %}
    {{ _log_check(qualify, run_id,
      'COMPL_MASTER_CORE_FIELDS', 'Completeness', 'customer_master', 'CRITICAL',
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_master)",
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_master WHERE full_name IS NULL OR email IS NULL)"
    ) }}
  {% endset %}
  {% do run_query(q) %}

  -- ==========================================================================
  -- UNIQUENESS: duplicate rate before/after resolution; no duplicate master IDs
  -- ==========================================================================
  {% set q %}
    {{ _log_check(qualify, run_id,
      'UNIQ_NO_DUPLICATE_MASTER_IDS', 'Uniqueness', 'customer_master', 'CRITICAL',
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_master)",
      "(SELECT COUNT(*) - COUNT(DISTINCT master_customer_id) FROM " ~ qualify ~ ".customer_master)"
    ) }}
  {% endset %}
  {% do run_query(q) %}

  {% set q %}
    {{ _log_check(qualify, run_id,
      'UNIQ_DEDUP_REDUCTION_RATE', 'Uniqueness', 'customer_master', 'INFO',
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_incoming_batch)",
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_incoming_batch) - (SELECT COUNT(DISTINCT resolved_master_id) FROM " ~ qualify ~ ".customer_incoming_batch)",
      "'failed count here = number of source rows collapsed into an existing master; higher is more dedup happening, not a data quality problem'"
    ) }}
  {% endset %}
  {% do run_query(q) %}

  -- ==========================================================================
  -- VALIDITY: email/MSISDN/ID format, date ranges
  -- ==========================================================================
  {% set q %}
    {{ _log_check(qualify, run_id,
      'VALID_EMAIL_FORMAT', 'Validity', 'customer_master', 'WARNING',
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_master WHERE email IS NOT NULL)",
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_master WHERE email IS NOT NULL AND email NOT RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$')"
    ) }}
  {% endset %}
  {% do run_query(q) %}

  {% set q %}
    {{ _log_check(qualify, run_id,
      'VALID_MSISDN_E164', 'Validity', 'customer_master', 'WARNING',
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_master WHERE msisdn_e164 IS NOT NULL)",
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_master WHERE msisdn_e164 IS NOT NULL AND msisdn_e164 NOT RLIKE '^\\\\+[1-9][0-9]{7,14}$')"
    ) }}
  {% endset %}
  {% do run_query(q) %}

  {% set q %}
    {{ _log_check(qualify, run_id,
      'VALID_NATIONAL_ID_LENGTH', 'Validity', 'customer_master', 'INFO',
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_master WHERE national_id IS NOT NULL)",
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_master WHERE national_id IS NOT NULL AND LENGTH(national_id) != 13)",
      "'assumes 13-digit SA ID numbers - adjust if your national_id format differs'"
    ) }}
  {% endset %}
  {% do run_query(q) %}

  -- ==========================================================================
  -- ACCURACY: does the survived value actually come from the priority
  -- source when that source has a record linked? (validates survivorship
  -- ran correctly, not accuracy against independent ground truth - true
  -- accuracy needs a trusted external reference this project doesn't have)
  -- ==========================================================================
  {% set q %}
    {{ _log_check(qualify, run_id,
      'ACC_IDENTITY_SURVIVORSHIP_CRM_PRIORITY', 'Accuracy', 'customer_master', 'WARNING',
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_master m JOIN " ~ qualify ~ ".customer_xref x ON m.master_customer_id = x.master_customer_id AND x.source_system = 'CRM' AND x.is_active = true)",
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_master m JOIN " ~ qualify ~ ".customer_xref x ON m.master_customer_id = x.master_customer_id AND x.source_system = 'CRM' AND x.is_active = true WHERE m.full_name_source != 'CRM')",
      "'when a CRM record is linked, survivorship should always prefer CRM for identity fields - any failures here indicate a survivorship bug'"
    ) }}
  {% endset %}
  {% do run_query(q) %}

  -- ==========================================================================
  -- CONSISTENCY: same attribute reconciles across sources (city, captured
  -- independently by both CRM and Ecommerce)
  -- ==========================================================================
  {% set q %}
    {{ _log_check(qualify, run_id,
      'CONSIST_CITY_CRM_VS_ECOMMERCE', 'Consistency', 'customer_master', 'INFO',
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_incoming_batch crm JOIN " ~ qualify ~ ".customer_incoming_batch ecom ON crm.resolved_master_id = ecom.resolved_master_id WHERE crm.source_system='CRM' AND ecom.source_system='ECOMMERCE' AND crm.city_norm IS NOT NULL AND ecom.city_norm IS NOT NULL)",
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_incoming_batch crm JOIN " ~ qualify ~ ".customer_incoming_batch ecom ON crm.resolved_master_id = ecom.resolved_master_id WHERE crm.source_system='CRM' AND ecom.source_system='ECOMMERCE' AND crm.city_norm IS NOT NULL AND ecom.city_norm IS NOT NULL AND crm.city_norm != ecom.city_norm)"
    ) }}
  {% endset %}
  {% do run_query(q) %}

  -- ==========================================================================
  -- TIMELINESS: ingestion freshness per feed. Uses each source's own
  -- updated/reference date as a proxy - wire this to your actual bronze
  -- _ingested_at column (see the Auto Loader pipeline) for true ingestion
  -- latency rather than business-date freshness.
  -- ==========================================================================
  {% set q %}
    {{ _log_check(qualify, run_id,
      'TIME_CRM_FRESHNESS_SLA', 'Timeliness', 'stg_crm_customers', 'INFO',
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_incoming_batch WHERE source_system='CRM')",
      "(SELECT COUNT(*) FROM " ~ qualify ~ ".customer_incoming_batch WHERE source_system='CRM' AND source_updated_at < date_sub(current_date(), " ~ var('dq_freshness_sla_days', 400) ~ "))",
      "'proxy check using CRM.created_date - replace with real ingestion timestamp for a true SLA measure'"
    ) }}
  {% endset %}
  {% do run_query(q) %}

  {{ log("Data quality checks complete. Run ID: " ~ run_id, info=true) }}

{% endmacro %}