{{ config(tags=['monthly-full', 'nice-history']) }}

{% set ns = namespace(calculation=nice_ind320('by_month')) %}
{% for model, fixture in [
    ('int_nice_reference_population_by_month', 'synthetic_population'),
    ('int_nice_ltc_population_by_month', 'synthetic_ltc'),
    ('int_lipid_lowering_medications_all', 'synthetic_orders'),
    ('int_bmi_qof_all', 'synthetic_recorded'),
    ('int_bmi_all', 'synthetic_calculated'),
    ('int_cholesterol_ldl_all', 'synthetic_ldl'),
    ('int_cholesterol_hdl_all', 'synthetic_hdl'),
    ('int_triglycerides_all', 'synthetic_triglycerides')
] %}
    {% set ns.calculation = ns.calculation | replace(ref(model) | string, fixture) %}
{% endfor %}

WITH synthetic_population AS (
    SELECT
        people.column1::NUMBER AS person_id,
        dates.column1::DATE AS reporting_date,
        CASE people.column1 WHEN -7737 THEN 18 WHEN -7738 THEN 85
            WHEN -7739 THEN 24 ELSE 40 END AS age,
        '1986-01-15'::DATE AS birth_date_approx,
        people.column2::VARCHAR AS gender,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM VALUES (-7731, 'Female'), (-7732, 'Female'), (-7733, 'Female'),
        (-7734, 'Male'), (-7735, 'Unknown'), (-7736, 'Female'), (-7737, 'Female'),
        (-7738, 'Female'), (-7739, 'Female'), (-7740, 'Female') AS people
    CROSS JOIN (VALUES ('2026-09-30'), ('2026-10-31')) AS dates
),
synthetic_ltc AS (
    SELECT person_id, reporting_date,
        FALSE AS has_chd, FALSE AS has_stroke_tia, FALSE AS has_diabetes,
        FALSE AS has_ndh, FALSE AS has_hypertension, FALSE AS has_pad,
        FALSE AS has_heart_failure, FALSE AS has_copd, FALSE AS has_learning_disability,
        FALSE AS has_obstructive_sleep_apnoea, FALSE AS has_smi
    FROM synthetic_population
),
synthetic_orders AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS order_date,
        column3::VARCHAR AS lipid_lowering_class
    FROM VALUES
        (-7737, '2026-03-30', 'STATIN'),
        (-7738, '2026-09-30', 'EZETIMIBE'),
        (-7738, '2026-09-30', 'STATIN'),
        (-7739, '2026-10-01', 'STATIN'),
        (-7740, '2026-09-30', 'OMEGA_3')
),
synthetic_recorded AS (
    SELECT NULL::NUMBER AS person_id, NULL::DATE AS clinical_effective_date,
        NULL::FLOAT AS bmi_value, 'BMIVAL_COD' AS source_cluster_id
    WHERE FALSE
),
synthetic_calculated AS (
    SELECT NULL::NUMBER AS person_id, NULL::DATE AS clinical_effective_date,
        FALSE AS is_valid_bmi, 'calculated' AS bmi_source
    WHERE FALSE
),
synthetic_ldl AS (
    SELECT column1::NUMBER AS person_id, column2::VARCHAR AS id,
        column3::TIMESTAMP_NTZ AS clinical_effective_date,
        4.1::FLOAT AS cholesterol_value, column4::BOOLEAN AS is_valid_cholesterol
    FROM VALUES
        (-7731, 'SYN_L1', '2025-09-30 12:00:00', TRUE),
        (-7732, 'SYN_L2', '2026-09-01 12:00:00', TRUE),
        (-7732, 'SYN_L3', '2026-09-01 13:00:00', FALSE),
        (-7736, 'SYN_L4', '2026-09-01 12:00:00', TRUE),
        (-7736, 'SYN_L5', '2026-09-01 12:00:00', FALSE)
),
synthetic_triglycerides AS (
    SELECT -7733::NUMBER AS person_id, 'SYN_T1'::VARCHAR AS id,
        '2026-10-01'::TIMESTAMP_NTZ AS clinical_effective_date,
        1.7::FLOAT AS triglycerides_value, TRUE AS is_valid_triglycerides
),
synthetic_hdl AS (
    SELECT column1::NUMBER AS person_id, 'SYN_H1'::VARCHAR AS id,
        '2026-09-01'::TIMESTAMP_NTZ AS clinical_effective_date,
        0.9::FLOAT AS cholesterol_value, TRUE AS is_valid_cholesterol
    FROM VALUES (-7734), (-7735)
),
actual AS ({{ ns.calculation }})
SELECT COUNT(*) AS failure_count
FROM actual
HAVING COUNT(*) <> 10
    OR COUNT_IF(person_id = -7731 AND reporting_date = '2026-09-30') <> 1
    OR COUNT_IF(person_id = -7733 AND reporting_date = '2026-10-31') <> 1
    OR COUNT_IF(person_id = -7734) <> 2
    OR COUNT_IF(person_id = -7736) <> 2
    OR COUNT_IF(person_id = -7737 AND reporting_date = '2026-09-30') <> 1
    OR COUNT_IF(person_id = -7738) <> 2
    OR COUNT_IF(person_id = -7739 AND reporting_date = '2026-10-31') <> 1
    OR COUNT_IF(person_id = -7740) <> 0
