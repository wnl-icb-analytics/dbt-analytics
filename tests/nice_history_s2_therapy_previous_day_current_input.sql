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
    FROM VALUES (DATEADD(day, -1, CURRENT_DATE()))
),
synthetic_orders AS (
    SELECT
        -9201::NUMBER AS person_id,
        'SYN_ORDER'::VARCHAR AS medication_order_id,
        column1::DATE AS order_date,
        'Synthetic product'::VARCHAR AS bnf_name
    FROM VALUES (DATEADD(day, -2, CURRENT_DATE()))
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
    FROM synthetic_orders
),
synthetic_ras AS (
    SELECT * FROM synthetic_orders
),
synthetic_sglt2 AS (
    SELECT *, 'DAPAGLIFLOZIN'::VARCHAR AS sglt2_drug
    FROM synthetic_orders
),
synthetic_antiplatelet AS (
    SELECT * FROM synthetic_orders
),
synthetic_anticoagulant AS (
    SELECT *, 'DOAC'::VARCHAR AS anticoagulant_type, TRUE AS is_doac, FALSE AS is_vka
    FROM synthetic_orders
    UNION ALL
    SELECT * REPLACE ('SYN_VKA'::VARCHAR AS medication_order_id),
        'VKA'::VARCHAR AS anticoagulant_type, FALSE AS is_doac, TRUE AS is_vka
    FROM synthetic_orders
),
stored_profile AS ({{ query.sql }}),
current_population AS (
    SELECT -9201::NUMBER AS person_id, CURRENT_DATE()::DATE AS reporting_date
),
expected AS (
    SELECT * REPLACE (CURRENT_DATE()::DATE AS reporting_date) FROM stored_profile
),
actual AS (
    SELECT profile.*
    FROM {{ nice_ref('int_nice_therapy_evidence', 'current') | replace(ref('int_nice_therapy_evidence') | string, 'stored_profile') }} AS profile
    INNER JOIN current_population AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
),
differences AS (
    (SELECT * FROM expected EXCEPT SELECT * FROM actual)
    UNION ALL
    (SELECT * FROM actual EXCEPT SELECT * FROM expected)
)
SELECT COUNT(*) AS rows_total
FROM differences
HAVING COUNT(*) <> 0
    OR (SELECT COUNT(*) FROM actual) <> 1
