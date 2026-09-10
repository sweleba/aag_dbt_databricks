{#
  Resolves product identity across PIM, ERP, and POS. Much simpler than
  customer resolution: product names are near-identical across sources
  (just differently cased - "Airtime Pro 20" vs "AIRTIME PRO 20"), so
  normalized-name matching alone is reliable. Same sequential
  seed-then-match pattern as customer resolution, for the same first-load
  reason: PIM goes first (richest attributes: category, price, status,
  launch date), ERP and POS match against what PIM seeds.
#}
{% macro resolve_product_identity() %}

  {% set catalog = var('catalog') %}
  {% set schema = var('silver_schema') %}
  {% set fuzzy_threshold = var('product_fuzzy_threshold') %}
  {% set qualify = catalog ~ "." ~ schema %}

  {% set build_batch %}
    CREATE OR REPLACE TABLE {{ qualify }}.product_incoming_batch AS
    SELECT
      source_code, source_system, product_name, product_name_norm,
      product_family, price, status, source_updated_at,
      CAST(NULL AS STRING) AS resolved_master_id,
      CAST(NULL AS STRING) AS match_rule,
      CAST(NULL AS DOUBLE) AS match_confidence,
      CAST(false AS BOOLEAN) AS is_new_master
    FROM {{ ref('stg_all_products') }}
  {% endset %}
  {% do run_query(build_batch) %}

  {% for sys in ['PIM', 'ERP', 'POS'] %}

    {{ log("Resolving product identity for source: " ~ sys, info=true) }}

    -- Tier 1: exact normalized name match
    {% set tier1 %}
      MERGE INTO {{ qualify }}.product_incoming_batch AS inc
      USING (
        SELECT product_name_norm, master_product_id
        FROM {{ qualify }}.product_master
        WHERE product_name_norm IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY product_name_norm ORDER BY updated_at DESC) = 1
      ) AS m
      ON inc.product_name_norm = m.product_name_norm
      WHEN MATCHED AND inc.source_system = '{{ sys }}' AND inc.resolved_master_id IS NULL
      THEN UPDATE SET
        inc.resolved_master_id = m.master_product_id,
        inc.match_rule = 'NAME_EXACT',
        inc.match_confidence = 1.0
    {% endset %}
    {% do run_query(tier1) %}

    -- Tier 2: fuzzy name match, for anything exact matching missed
    -- (e.g. minor punctuation/spacing differences)


        {% set tier2 %}
      MERGE INTO {{ qualify }}.product_incoming_batch AS inc
      USING (
        WITH scored AS (
          SELECT
            i.source_code,
            m.master_product_id,
            1 - (levenshtein(i.product_name_norm, m.product_name_norm)
                 / greatest(length(i.product_name_norm), length(m.product_name_norm), 1)) AS match_score
          FROM {{ qualify }}.product_incoming_batch i
          JOIN {{ qualify }}.product_master m ON 1 = 1  -- small table (~60 rows); no blocking key needed
          WHERE i.source_system = '{{ sys }}' AND i.resolved_master_id IS NULL
        ),
        ranked AS (
          SELECT
            source_code, master_product_id, match_score,
            ROW_NUMBER() OVER (PARTITION BY source_code ORDER BY match_score DESC) AS rn
          FROM scored
        )
        SELECT source_code, master_product_id, match_score
        FROM ranked
        WHERE rn = 1 AND match_score >= {{ fuzzy_threshold }}
      ) AS cand
      ON inc.source_code = cand.source_code
      WHEN MATCHED AND inc.source_system = '{{ sys }}' AND inc.resolved_master_id IS NULL
      THEN UPDATE SET
        inc.resolved_master_id = cand.master_product_id,
        inc.match_rule = 'NAME_FUZZY',
        inc.match_confidence = cand.match_score
    {% endset %}
    {% do run_query(tier2) %}




        -- Tier 3: no match -> new product master.
        -- Same fix as customer resolution: uuid() can't sit directly in an
        -- UPDATE/MERGE SET clause on Delta - generate via SELECT first.
        {% set tier3_generate_ids %}
        CREATE OR REPLACE TEMPORARY VIEW new_product_ids AS
        SELECT
            source_system, source_code,
            concat('PROD-', replace(uuid(), '-', '')) AS new_master_id
        FROM {{ qualify }}.product_incoming_batch
        WHERE source_system = '{{ sys }}' AND resolved_master_id IS NULL
        {% endset %}
        {% do run_query(tier3_generate_ids) %}

        {% set tier3_assign_ids %}
        MERGE INTO {{ qualify }}.product_incoming_batch AS inc
        USING new_product_ids AS nid
        ON inc.source_system = nid.source_system AND inc.source_code = nid.source_code
        WHEN MATCHED THEN UPDATE SET
            inc.resolved_master_id = nid.new_master_id,
            inc.is_new_master = true
        {% endset %}
        {% do run_query(tier3_assign_ids) %}
    
  {% endfor %}

  -- Crosswalk upsert
  {% set upsert_xref %}
    MERGE INTO {{ qualify }}.product_xref AS x
    USING {{ qualify }}.product_incoming_batch AS inc
    ON x.source_system = inc.source_system AND x.source_code = inc.source_code
    WHEN MATCHED THEN UPDATE SET
      x.master_product_id = inc.resolved_master_id,
      x.match_rule = inc.match_rule,
      x.match_confidence = inc.match_confidence,
      x.matched_at = current_timestamp(),
      x.is_active = true
    WHEN NOT MATCHED THEN INSERT (
      master_product_id, source_system, source_code, match_rule, match_confidence, matched_at, is_active
    ) VALUES (
      inc.resolved_master_id, inc.source_system, inc.source_code, inc.match_rule, inc.match_confidence, current_timestamp(), true
    )
  {% endset %}
  {% do run_query(upsert_xref) %}

  -- Survivorship: prefer PIM attributes, fall back to ERP then POS
  {% set survivorship %}
    MERGE INTO {{ qualify }}.product_master AS m
    USING (
      SELECT
        x.master_product_id,
        COALESCE(pim.product_name, erp.product_name, pos.product_name) AS product_name,
        COALESCE(pim.product_name_norm, erp.product_name_norm, pos.product_name_norm) AS product_name_norm,
        COALESCE(pim.product_family, erp.product_family, pos.product_family) AS product_family,
        COALESCE(pim.price, erp.price, pos.price) AS list_price
      FROM (SELECT DISTINCT resolved_master_id AS master_product_id FROM {{ qualify }}.product_incoming_batch) x
      LEFT JOIN {{ qualify }}.product_incoming_batch pim
        ON pim.resolved_master_id = x.master_product_id AND pim.source_system = 'PIM'
      LEFT JOIN {{ qualify }}.product_incoming_batch erp
        ON erp.resolved_master_id = x.master_product_id AND erp.source_system = 'ERP'
      LEFT JOIN {{ qualify }}.product_incoming_batch pos
        ON pos.resolved_master_id = x.master_product_id AND pos.source_system = 'POS'
    ) AS s
    ON m.master_product_id = s.master_product_id
    WHEN MATCHED THEN UPDATE SET
      m.product_name = COALESCE(s.product_name, m.product_name),
      m.product_name_norm = COALESCE(s.product_name_norm, m.product_name_norm),
      m.product_family = COALESCE(s.product_family, m.product_family),
      m.list_price = COALESCE(s.list_price, m.list_price),
      m.updated_at = current_timestamp()
  {% endset %}
  {% do run_query(survivorship) %}

  {{ log("Product identity resolution complete.", info=true) }}

{% endmacro %}
