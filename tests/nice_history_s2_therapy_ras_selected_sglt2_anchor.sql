{{ config(tags=['monthly-full', 'nice-history']) }}

{% set query = namespace(sql=calculate_nice_therapy_evidence('by_month')) %}
{% set query.sql = query.sql | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set query.sql = query.sql | replace(nice_ltc_summary('by_month') | string, 'SELECT person_id, reporting_date FROM synthetic_population WHERE FALSE') %}
{% for model, fixture in {'int_lipid_lowering_medications_all': 'synthetic_llt', 'int_ace_inhibitor_medications_all': 'synthetic_ras', 'int_arb_medications_all': 'synthetic_empty_orders', 'int_sglt2_medications_all': 'synthetic_sglt2', 'int_antiplatelet_medications_all': 'synthetic_antiplatelet', 'int_anticoagulant_medications_all': 'synthetic_anticoagulant'}.items() %}
    {% set query.sql = query.sql | replace(ref(model) | string, fixture) %}
{% endfor %}

WITH synthetic_population AS (
    SELECT
        -9201::NUMBER AS person_id,
        column1::DATE AS reporting_date,
        30 AS age
    FROM VALUES ('2024-02-29'), ('2024-03-31')
),
synthetic_orders AS (
    SELECT
        -9201::NUMBER AS person_id,
        'SYN_ORDER'::VARCHAR AS medication_order_id,
        column1::DATE AS order_date,
        'Synthetic product'::VARCHAR AS bnf_name
    FROM VALUES ('2024-01-31'), ('2024-02-29'), ('1989-12-31')
),
synthetic_empty_orders AS (
    SELECT * FROM synthetic_orders WHERE FALSE
),
synthetic_llt AS (
    SELECT
        *,
        'STATIN'::VARCHAR AS lipid_lowering_class,
        TRUE AS is_statin,
        'HIGH_INTENSITY'::VARCHAR AS statin_intensity
    FROM synthetic_empty_orders
),
synthetic_ras AS (
    SELECT * FROM synthetic_orders
),
synthetic_sglt2 AS (
    SELECT * REPLACE (
        CASE WHEN order_date = '2024-02-29' THEN '2024-03-01'::DATE ELSE order_date END AS order_date
    ), 'DAPAGLIFLOZIN'::VARCHAR AS sglt2_drug
    FROM synthetic_orders
    WHERE order_date IN ('2024-01-31', '2024-02-29')
),
synthetic_antiplatelet AS (
    SELECT * FROM synthetic_empty_orders
),
synthetic_anticoagulant AS (
    SELECT *, 'DOAC'::VARCHAR AS anticoagulant_type, TRUE AS is_doac, FALSE AS is_vka
    FROM synthetic_empty_orders
),
actual AS ({{ query.sql }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 2
    OR COALESCE(COUNT_IF(reporting_date = '2024-02-29' AND latest_ras_order_date = '2024-02-29'
        AND latest_sglt2_order_date = '2024-01-31'
        AND latest_ras_order_before_last_sglt2_date = '2024-01-31'), 0) <> 1
    OR COALESCE(COUNT_IF(reporting_date = '2024-03-31' AND latest_sglt2_order_date = '2024-03-01'
        AND latest_ras_order_before_last_sglt2_date = '2024-02-29'), 0) <> 1
