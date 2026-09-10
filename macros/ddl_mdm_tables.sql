{#
  Creates every persistent table the Silver layer depends on, if they don't
  already exist. Safe to run every time - CREATE TABLE IF NOT EXISTS means
  existing master/crosswalk data is never touched by this macro.
#}
{% macro create_mdm_tables() %}

  {% set catalog = var('catalog') %}
  {% set schema = var('silver_schema') %}

  {% do run_query("CREATE SCHEMA IF NOT EXISTS " ~ catalog ~ "." ~ schema) %}

  -- ==========================================================================
  -- CUSTOMER: persistent golden record. master_customer_id is generated once
  -- and never regenerated on reload - that persistence is what this table
  -- being separate from any dbt model's normal materialization achieves.
  -- ==========================================================================
  {% set ddl_customer_master %}
    CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.customer_master (
      master_customer_id     STRING NOT NULL,
      full_name               STRING,
      full_name_norm           STRING,
      full_name_source        STRING,
      email                     STRING,
      email_source              STRING,
      msisdn_e164               STRING,
      msisdn_source              STRING,
      national_id                STRING,
      national_id_source          STRING,
      date_of_birth                DATE,
      date_of_birth_source           STRING,
      city_norm                      STRING,
      city_source                     STRING,
      outstanding_balance              DECIMAL(12,2),
      currency                          STRING,
      marketing_opt_in                   BOOLEAN,
      match_confidence                    DOUBLE,
      is_inferred                          BOOLEAN,   -- true = stub row created for a fact
                                                        -- that arrived before any real source
                                                        -- record existed (late-arriving dimension)
      created_at                           TIMESTAMP,
      updated_at                            TIMESTAMP
    ) USING DELTA
  {% endset %}
  {% do run_query(ddl_customer_master) %}

  -- Crosswalk: every source row's link to its resolved master. This is the
  -- audit trail - keep every row, including inactive ones from unmerges.
  {% set ddl_customer_xref %}
    CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.customer_xref (
      master_customer_id  STRING NOT NULL,
      source_system         STRING NOT NULL,
      source_id               STRING NOT NULL,
      match_rule               STRING,
      match_confidence          DOUBLE,
      matched_at                 TIMESTAMP,
      is_active                   BOOLEAN
    ) USING DELTA
  {% endset %}
  {% do run_query(ddl_customer_xref) %}

  -- Stewardship queue: borderline fuzzy matches (Tier 4, below auto-merge
  -- threshold) for human review. A provisional new master is still created
  -- so downstream facts have something to join to - see README for why.
  {% set ddl_customer_stewardship %}
    CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.customer_stewardship_queue (
      queue_id                       STRING NOT NULL,
      source_system                    STRING,
      source_id                         STRING,
      provisional_master_customer_id     STRING,
      candidate_master_customer_id        STRING,
      match_rule                           STRING,
      match_confidence                      DOUBLE,
      reason                                 STRING,
      status                                  STRING,   -- PENDING, APPROVED, REJECTED
      created_at                               TIMESTAMP,
      resolved_at                               TIMESTAMP,
      resolved_by                                STRING
    ) USING DELTA
  {% endset %}
  {% do run_query(ddl_customer_stewardship) %}

  -- Manual overrides: how a steward corrects an automated decision.
  -- FORCE_MERGE points source_id at a specific target master.
  -- FORCE_SPLIT gives source_id back its own independent master (unmerge).
  {% set ddl_customer_overrides %}
    CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.customer_merge_overrides (
      override_id                 STRING NOT NULL,
      source_system                 STRING NOT NULL,
      source_id                       STRING NOT NULL,
      action                            STRING NOT NULL,  -- FORCE_MERGE | FORCE_SPLIT
      target_master_customer_id          STRING,           -- required for FORCE_MERGE
      reason                               STRING,
      applied_by                            STRING,
      applied_at                             TIMESTAMP,
      is_active                               BOOLEAN
    ) USING DELTA
  {% endset %}
  {% do run_query(ddl_customer_overrides) %}

  -- ==========================================================================
  -- PRODUCT: same master + crosswalk pattern, simpler because product names
  -- match near-exactly across PIM/ERP/POS once cased consistently.
  -- ==========================================================================
  {% set ddl_product_master %}
    CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.product_master (
      master_product_id    STRING NOT NULL,
      product_name           STRING,
      product_name_norm       STRING,
      product_family            STRING,
      list_price                 DECIMAL(12,2),
      match_confidence             DOUBLE,
      created_at                    TIMESTAMP,
      updated_at                     TIMESTAMP
    ) USING DELTA
  {% endset %}
  {% do run_query(ddl_product_master) %}

  {% set ddl_product_xref %}
    CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.product_xref (
      master_product_id   STRING NOT NULL,
      source_system          STRING NOT NULL,
      source_code               STRING NOT NULL,
      match_rule                  STRING,
      match_confidence              DOUBLE,
      matched_at                     TIMESTAMP,
      is_active                        BOOLEAN
    ) USING DELTA
  {% endset %}
  {% do run_query(ddl_product_xref) %}

  -- ==========================================================================
  -- DATA QUALITY: one shared results table for every check, across entities.
  -- ==========================================================================
  {% set ddl_dq_results %}
    CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.dq_results (
      check_run_id     STRING NOT NULL,
      rule_id            STRING NOT NULL,
      dimension            STRING NOT NULL,  -- Completeness | Uniqueness | Validity | Accuracy | Consistency | Timeliness
      entity                 STRING NOT NULL,  -- e.g. 'customer_master', 'crm_customers'
      severity                 STRING NOT NULL,  -- INFO | WARNING | CRITICAL
      records_tested             BIGINT,
      records_failed               BIGINT,
      pass_rate                     DOUBLE,
      run_timestamp                  TIMESTAMP,
      details                          STRING
    ) USING DELTA
  {% endset %}
  {% do run_query(ddl_dq_results) %}

{% endmacro %}
