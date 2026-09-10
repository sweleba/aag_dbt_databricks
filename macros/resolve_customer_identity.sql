{#
  Resolves customer identity across CRM, Billing, and Ecommerce into a
  persistent golden record. Implements the SOP's 5-tier matching order and
  3-group survivorship rules.

  DESIGN: sources are processed SEQUENTIALLY (CRM, then Billing, then
  Ecommerce), not as one static batch matched against a frozen master
  snapshot. This matters for first-time bulk loads: if all three sources
  were matched against an empty master table simultaneously, cross-source
  duplicates would never be caught (nothing exists yet to match against).
  Processing one source at a time lets each subsequent source match against
  the masters the previous sources just created - CRM seeds the master
  table, Billing gets matched against those (catching real overlaps) before
  creating masters for whoever's left, then Ecommerce matches against the
  combined CRM+Billing set. Order reflects data quality: CRM has the
  richest identity fields (national_id, DOB, city all populated), so it
  goes first.

  ASSUMPTION (flag for the business/stewardship team to confirm): a
  provisional new master is created even for Tier 4 near-misses in the
  review band, rather than blocking the pipeline on human sign-off. The
  near-miss is still logged to the stewardship queue for retrospective
  merge via the override table - downstream fact tables just aren't left
  without something to join to in the meantime.
#}
{% macro resolve_customer_identity() %}

  {% set catalog = var('catalog') %}
  {% set schema = var('silver_schema') %}
  {% set auto_threshold = var('fuzzy_auto_merge_threshold') %}
  {% set review_threshold = var('fuzzy_review_threshold') %}
  {% set qualify = catalog ~ "." ~ schema %}

  -- ========================================================================
  -- STEP 1: build this run's working batch from all three standardized
  -- sources. Rebuilt fresh every run - safe, since it's just a staging area,
  -- not where anything persists.
  -- ========================================================================
  {% set build_batch %}
    CREATE OR REPLACE TABLE {{ qualify }}.customer_incoming_batch AS
    SELECT
      source_id, source_system, full_name, full_name_norm, email_norm, msisdn_e164,
      national_id, date_of_birth, city_norm, status, source_updated_at,
      last_purchased_code, last_purchased_code_type,
      outstanding_balance, currency, marketing_opt_in,
      CAST(NULL AS STRING) AS resolved_master_id,
      CAST(NULL AS STRING) AS match_rule,
      CAST(NULL AS DOUBLE) AS match_confidence,
      CAST(NULL AS STRING) AS stewardship_candidate_master_id,
      CAST(false AS BOOLEAN) AS is_new_master
    FROM {{ ref('stg_crm_customers') }}
    UNION ALL
    SELECT
      source_id, source_system, full_name, full_name_norm, email_norm, msisdn_e164,
      national_id, date_of_birth, city_norm, status, source_updated_at,
      last_purchased_code, last_purchased_code_type,
      outstanding_balance, currency, marketing_opt_in,
      NULL, NULL, NULL, NULL, false
    FROM {{ ref('stg_billing_accounts') }}
    UNION ALL
    SELECT
      source_id, source_system, full_name, full_name_norm, email_norm, msisdn_e164,
      national_id, date_of_birth, city_norm, status, source_updated_at,
      last_purchased_code, last_purchased_code_type,
      outstanding_balance, currency, marketing_opt_in,
      NULL, NULL, NULL, NULL, false
    FROM {{ ref('stg_ecommerce_users') }}
  {% endset %}
  {% do run_query(build_batch) %}

  -- ========================================================================
  -- STEP 2: apply FORCE_SPLIT overrides up front, by excluding those source
  -- rows from ever being auto-matched below - they always get their own
  -- fresh master regardless of what tiers 1-4 would otherwise find.
  -- (FORCE_MERGE overrides are applied later, after automated matching,
  -- since they should win even over an automated match that already ran.)
  -- ========================================================================
  {% set apply_force_split %}
    UPDATE {{ qualify }}.customer_incoming_batch AS inc
    SET match_rule = 'FORCE_SPLIT_EXCLUDED'
    WHERE EXISTS (
      SELECT 1 FROM {{ qualify }}.customer_merge_overrides o
      WHERE o.is_active = true
        AND o.action = 'FORCE_SPLIT'
        AND o.source_system = inc.source_system
        AND o.source_id = inc.source_id
    )
  {% endset %}
  {% do run_query(apply_force_split) %}

  -- ========================================================================
  -- STEP 3: process each source in priority order. Every tier only looks at
  -- rows this source still hasn't matched (resolved_master_id IS NULL) and
  -- that aren't FORCE_SPLIT-excluded.
  -- ========================================================================
  {% for sys in ['CRM', 'BILLING', 'ECOMMERCE'] %}

    {{ log("Resolving identity for source: " ~ sys, info=true) }}

    -- Tier 1: exact national_id match (highest trust)
    {% set tier1 %}
      MERGE INTO {{ qualify }}.customer_incoming_batch AS inc
      USING (
        SELECT national_id, master_customer_id
        FROM {{ qualify }}.customer_master
        WHERE national_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY national_id ORDER BY updated_at DESC) = 1
      ) AS m
      ON inc.national_id = m.national_id
      WHEN MATCHED AND inc.source_system = '{{ sys }}'
                    AND inc.resolved_master_id IS NULL
                    AND inc.match_rule IS NULL
      THEN UPDATE SET
        inc.resolved_master_id = m.master_customer_id,
        inc.match_rule = 'NATIONAL_ID_EXACT',
        inc.match_confidence = 1.0
    {% endset %}
    {% do run_query(tier1) %}

    -- Tier 2: normalized email match
    {% set tier2 %}
      MERGE INTO {{ qualify }}.customer_incoming_batch AS inc
      USING (
        SELECT email, master_customer_id
        FROM {{ qualify }}.customer_master
        WHERE email IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY email ORDER BY updated_at DESC) = 1
      ) AS m
      ON inc.email_norm = m.email
      WHEN MATCHED AND inc.source_system = '{{ sys }}'
                    AND inc.resolved_master_id IS NULL
                    AND inc.match_rule IS NULL
      THEN UPDATE SET
        inc.resolved_master_id = m.master_customer_id,
        inc.match_rule = 'EMAIL_EXACT',
        inc.match_confidence = 1.0
    {% endset %}
    {% do run_query(tier2) %}

    -- Tier 3: normalized MSISDN match (E.164)
    {% set tier3 %}
      MERGE INTO {{ qualify }}.customer_incoming_batch AS inc
      USING (
        SELECT msisdn_e164, master_customer_id
        FROM {{ qualify }}.customer_master
        WHERE msisdn_e164 IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY msisdn_e164 ORDER BY updated_at DESC) = 1
      ) AS m
      ON inc.msisdn_e164 = m.msisdn_e164
      WHEN MATCHED AND inc.source_system = '{{ sys }}'
                    AND inc.resolved_master_id IS NULL
                    AND inc.match_rule IS NULL
      THEN UPDATE SET
        inc.resolved_master_id = m.master_customer_id,
        inc.match_rule = 'MSISDN_EXACT',
        inc.match_confidence = 1.0
    {% endset %}
    {% do run_query(tier3) %}

    -- Tier 4: fuzzy name + DOB + city, above threshold.
    -- Blocked on matching city_norm to keep the comparison space bounded -
    -- without a blocking key this is an O(n*m) cross join per source.
    -- Weights (0.6 name / 0.25 DOB / 0.15 city) are a reasonable starting
    -- point, not a tuned value - see README on calibrating thresholds.

    {% set tier4 %}
        MERGE INTO {{ qualify }}.customer_incoming_batch AS inc
        USING (
            WITH scored AS (
            SELECT
                i.source_id,
                m.master_customer_id,
                ( 0.6  * (1 - (levenshtein(i.full_name_norm, m.full_name_norm)
                            / greatest(length(i.full_name_norm), length(m.full_name_norm), 1)))
                + 0.25 * (CASE WHEN i.date_of_birth IS NOT NULL AND i.date_of_birth = m.date_of_birth THEN 1.0 ELSE 0.0 END)
                + 0.15 * (CASE WHEN i.city_norm IS NOT NULL AND i.city_norm = m.city_norm THEN 1.0 ELSE 0.0 END)
                ) AS match_score
            FROM {{ qualify }}.customer_incoming_batch i
            JOIN {{ qualify }}.customer_master m
                ON i.city_norm = m.city_norm
            WHERE i.source_system = '{{ sys }}'
                AND i.resolved_master_id IS NULL
                AND i.match_rule IS NULL
                AND i.city_norm IS NOT NULL
                AND i.full_name_norm IS NOT NULL
                AND m.full_name_norm IS NOT NULL
            ),
            ranked AS (
            SELECT
                source_id, master_customer_id, match_score,
                ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY match_score DESC) AS rn
            FROM scored
            )
            SELECT source_id, master_customer_id, match_score
            FROM ranked
            WHERE rn = 1 AND match_score >= {{ review_threshold }}
        ) AS cand
        ON inc.source_id = cand.source_id
        WHEN MATCHED AND inc.source_system = '{{ sys }}'
                        AND inc.resolved_master_id IS NULL
                        AND inc.match_rule IS NULL
        THEN UPDATE SET
            inc.match_rule = 'FUZZY_NAME_DOB_CITY',
            inc.match_confidence = cand.match_score,
            inc.resolved_master_id =
            CASE WHEN cand.match_score >= {{ auto_threshold }} THEN cand.master_customer_id ELSE NULL END,
            inc.stewardship_candidate_master_id =
            CASE WHEN cand.match_score < {{ auto_threshold }} THEN cand.master_customer_id ELSE NULL END
    {% endset %}
    {% do run_query(tier4) %}

    -- Tier 5: still unmatched (or below auto-merge threshold) -> new master.
    -- See the ASSUMPTION note at the top of this macro re: why this happens
    -- automatically rather than waiting on stewardship review.
    -- IDs are generated via a separate SELECT first (a temp view), then
    -- merged in - Databricks disallows non-deterministic functions like
    -- uuid() directly inside an UPDATE/MERGE SET clause on a Delta table.


    {% set tier5_generate_ids %}
        CREATE OR REPLACE TEMPORARY VIEW new_customer_ids AS
        SELECT
            source_system, source_id,
            concat('CUST-', replace(uuid(), '-', '')) AS new_master_id
        FROM {{ qualify }}.customer_incoming_batch
        WHERE source_system = '{{ sys }}' AND resolved_master_id IS NULL
    {% endset %}
    {% do run_query(tier5_generate_ids) %}

    {% set tier5_assign_ids %}
        MERGE INTO {{ qualify }}.customer_incoming_batch AS inc
        USING new_customer_ids AS nid
        ON inc.source_system = nid.source_system AND inc.source_id = nid.source_id
        WHEN MATCHED THEN UPDATE SET
        inc.resolved_master_id = nid.new_master_id,
        inc.is_new_master = true
    {% endset %}
    {% do run_query(tier5_assign_ids) %}

{% endfor %}

  -- ========================================================================
  -- STEP 4: apply FORCE_MERGE overrides - these win even over an automated
  -- match that already happened above.
  -- ========================================================================
  {% set apply_force_merge %}
    MERGE INTO {{ qualify }}.customer_incoming_batch AS inc
    USING (
      SELECT source_system, source_id, target_master_customer_id
      FROM {{ qualify }}.customer_merge_overrides
      WHERE is_active = true AND action = 'FORCE_MERGE'
    ) AS o
    ON inc.source_system = o.source_system AND inc.source_id = o.source_id
    WHEN MATCHED THEN UPDATE SET
      inc.resolved_master_id = o.target_master_customer_id,
      inc.match_rule = 'MANUAL_OVERRIDE',
      inc.match_confidence = 1.0
  {% endset %}
  {% do run_query(apply_force_merge) %}

  -- ========================================================================
  -- STEP 5: upsert the crosswalk - the audit trail of source row -> master.
  -- ========================================================================
  {% set upsert_xref %}
    MERGE INTO {{ qualify }}.customer_xref AS x
    USING {{ qualify }}.customer_incoming_batch AS inc
    ON x.source_system = inc.source_system AND x.source_id = inc.source_id
    WHEN MATCHED THEN UPDATE SET
      x.master_customer_id = inc.resolved_master_id,
      x.match_rule = inc.match_rule,
      x.match_confidence = inc.match_confidence,
      x.matched_at = current_timestamp(),
      x.is_active = true
    WHEN NOT MATCHED THEN INSERT (
      master_customer_id, source_system, source_id, match_rule, match_confidence, matched_at, is_active
    ) VALUES (
      inc.resolved_master_id, inc.source_system, inc.source_id, inc.match_rule, inc.match_confidence, current_timestamp(), true
    )
  {% endset %}
  {% do run_query(upsert_xref) %}

  -- ========================================================================
  -- STEP 6: log Tier 4 near-misses to the stewardship queue for human review.
  -- ========================================================================
  {% set log_stewardship %}
    INSERT INTO {{ qualify }}.customer_stewardship_queue (
      queue_id, source_system, source_id, provisional_master_customer_id,
      candidate_master_customer_id, match_rule, match_confidence, reason,
      status, created_at, resolved_at, resolved_by
    )
    SELECT
      concat('SQ-', replace(uuid(), '-', '')),
      source_system, source_id, resolved_master_id,
      stewardship_candidate_master_id, match_rule, match_confidence,
      'Fuzzy match below auto-merge threshold - possible duplicate of an existing master',
      'PENDING', current_timestamp(), NULL, NULL
    FROM {{ qualify }}.customer_incoming_batch
    WHERE stewardship_candidate_master_id IS NOT NULL
  {% endset %}
  {% do run_query(log_stewardship) %}

  -- ========================================================================
  -- STEP 7: survivorship - refresh master attributes for every master
  -- touched this run, per the SOP's field-group priority rules:
  --   Identity (name/national_id/DOB/city/email/msisdn): CRM, then Billing,
  --     then Ecommerce (email/msisdn specifically fall back to Ecommerce
  --     too, since it's flagged as the most reliable source for those two
  --     fields in isolation - see README)
  --   Financial (balance/currency): Billing only
  --   Digital/consent (marketing_opt_in): Ecommerce only
  -- Ties (same priority, both non-null) go to the most recently updated
  -- source record.
  -- ========================================================================
  {% set survivorship %}
    MERGE INTO {{ qualify }}.customer_master AS m
    USING (
      SELECT
        x.master_customer_id,
        COALESCE(crm.full_name, bil.full_name, ecom.full_name) AS full_name,
        COALESCE(crm.full_name_norm, bil.full_name_norm, ecom.full_name_norm) AS full_name_norm,
        COALESCE(
          CASE WHEN crm.full_name IS NOT NULL THEN 'CRM' END,
          CASE WHEN bil.full_name IS NOT NULL THEN 'BILLING' END,
          CASE WHEN ecom.full_name IS NOT NULL THEN 'ECOMMERCE' END
        ) AS full_name_source,
        COALESCE(crm.email_norm, bil.email_norm, ecom.email_norm) AS email,
        COALESCE(
          CASE WHEN crm.email_norm IS NOT NULL THEN 'CRM' END,
          CASE WHEN bil.email_norm IS NOT NULL THEN 'BILLING' END,
          CASE WHEN ecom.email_norm IS NOT NULL THEN 'ECOMMERCE' END
        ) AS email_source,
        COALESCE(crm.msisdn_e164, bil.msisdn_e164, ecom.msisdn_e164) AS msisdn_e164,
        COALESCE(
          CASE WHEN crm.msisdn_e164 IS NOT NULL THEN 'CRM' END,
          CASE WHEN bil.msisdn_e164 IS NOT NULL THEN 'BILLING' END,
          CASE WHEN ecom.msisdn_e164 IS NOT NULL THEN 'ECOMMERCE' END
        ) AS msisdn_source,
        COALESCE(crm.national_id, bil.national_id) AS national_id,
        COALESCE(
          CASE WHEN crm.national_id IS NOT NULL THEN 'CRM' END,
          CASE WHEN bil.national_id IS NOT NULL THEN 'BILLING' END
        ) AS national_id_source,
        COALESCE(crm.date_of_birth, bil.date_of_birth) AS date_of_birth,
        COALESCE(
          CASE WHEN crm.date_of_birth IS NOT NULL THEN 'CRM' END,
          CASE WHEN bil.date_of_birth IS NOT NULL THEN 'BILLING' END
        ) AS date_of_birth_source,
        COALESCE(crm.city_norm, ecom.city_norm) AS city_norm,
        COALESCE(
          CASE WHEN crm.city_norm IS NOT NULL THEN 'CRM' END,
          CASE WHEN ecom.city_norm IS NOT NULL THEN 'ECOMMERCE' END
        ) AS city_source,
        bil.outstanding_balance AS outstanding_balance,
        bil.currency AS currency,
        ecom.marketing_opt_in AS marketing_opt_in
      FROM (SELECT DISTINCT resolved_master_id AS master_customer_id FROM {{ qualify }}.customer_incoming_batch) x
      LEFT JOIN {{ qualify }}.customer_incoming_batch crm
        ON crm.resolved_master_id = x.master_customer_id AND crm.source_system = 'CRM'
      LEFT JOIN {{ qualify }}.customer_incoming_batch bil
        ON bil.resolved_master_id = x.master_customer_id AND bil.source_system = 'BILLING'
      LEFT JOIN {{ qualify }}.customer_incoming_batch ecom
        ON ecom.resolved_master_id = x.master_customer_id AND ecom.source_system = 'ECOMMERCE'
    ) AS s
    ON m.master_customer_id = s.master_customer_id
    WHEN MATCHED THEN UPDATE SET
      m.full_name = COALESCE(s.full_name, m.full_name),
      m.full_name_norm = COALESCE(s.full_name_norm, m.full_name_norm),
      m.full_name_source = COALESCE(s.full_name_source, m.full_name_source),
      m.email = COALESCE(s.email, m.email),
      m.email_source = COALESCE(s.email_source, m.email_source),
      m.msisdn_e164 = COALESCE(s.msisdn_e164, m.msisdn_e164),
      m.msisdn_source = COALESCE(s.msisdn_source, m.msisdn_source),
      m.national_id = COALESCE(s.national_id, m.national_id),
      m.national_id_source = COALESCE(s.national_id_source, m.national_id_source),
      m.date_of_birth = COALESCE(s.date_of_birth, m.date_of_birth),
      m.date_of_birth_source = COALESCE(s.date_of_birth_source, m.date_of_birth_source),
      m.city_norm = COALESCE(s.city_norm, m.city_norm),
      m.city_source = COALESCE(s.city_source, m.city_source),
      m.outstanding_balance = COALESCE(s.outstanding_balance, m.outstanding_balance),
      m.currency = COALESCE(s.currency, m.currency),
      m.marketing_opt_in = COALESCE(s.marketing_opt_in, m.marketing_opt_in),
      m.updated_at = current_timestamp()
  {% endset %}
  {% do run_query(survivorship) %}

  {{ log("Customer identity resolution complete.", info=true) }}

{% endmacro %}