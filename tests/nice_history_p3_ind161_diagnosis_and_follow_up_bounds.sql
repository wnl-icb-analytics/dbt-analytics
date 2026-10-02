{{ config(tags=['monthly-full', 'nice-history']) }}

{% set query = namespace(sql=nice_ind161('by_month')) %}
{% set query.sql = query.sql | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set query.sql = query.sql | replace(nice_ref('int_cvd_risk_profile', 'by_month') | string, 'synthetic_profile') %}
{% set query.sql = query.sql | replace(ref('int_cvd_risk_assessment_all') | string, 'synthetic_assessments') %}

WITH synthetic_population AS (
    SELECT
        people.column1::NUMBER AS person_id,
        dates.column1::DATE AS reporting_date,
        50 AS age,
        'SYNTHETIC'::VARCHAR AS practice_code,
        'Synthetic practice'::VARCHAR AS practice_name
    FROM VALUES (1), (2), (3), (4), (5) AS people
    CROSS JOIN (SELECT column1 FROM VALUES ('2024-01-31'), ('2024-02-29'), ('2024-04-30'), ('2025-02-01')) AS dates
),

synthetic_profile AS (
    SELECT
        population.person_id AS person_id,
        NULL::FLOAT AS latest_risk_score,
        NULL::DATE AS latest_risk_score_date,
        NULL::FLOAT AS latest_risk_score_type,
        NULL::FLOAT AS max_risk_score_ever,
        NULL::FLOAT AS max_risk_score_12m,
        NULL::FLOAT AS min_risk_score_36m,
        NULL::DATE AS latest_risk_assessment_date,
        NULL::FLOAT AS latest_cvd_risk_score,
        NULL::DATE AS latest_cvd_risk_score_date,
        NULL::FLOAT AS max_cvd_risk_score_12m,
        NULL::FLOAT AS latest_low_cvd_risk_score_date_36m,
        NULL::FLOAT AS latest_high_cvd_risk_score_date_36m,
        FALSE AS has_cvd,
        FALSE AS has_cvd_including_haemorrhagic_stroke,
        FALSE AS has_familial_hypercholesterolaemia,
        FALSE AS has_ckd,
        FALSE AS has_diabetes,
        FALSE AS has_type1_diabetes,
        FALSE AS has_type2_diabetes,
        '2024-02-29'::DATE AS earliest_type2_diabetes_date,
        FALSE AS has_hypertension,
        '2024-01-31'::DATE AS earliest_hypertension_date,
        NULL::VARCHAR AS latest_frailty_severity,
        NULL::DATE AS latest_lipid_lowering_order_date,
        NULL::DATE AS latest_statin_order_date,
        FALSE AS is_current_smoker,
        FALSE AS has_obesity,
        NULL::VARCHAR AS latest_total_cholesterol,
        NULL::DATE AS latest_total_cholesterol_date,
        population.reporting_date AS reporting_date
    FROM synthetic_population AS population
),

synthetic_assessments AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::TIMESTAMP_NTZ AS clinical_effective_date
    FROM VALUES (1, '2024-02-29'), (2, '2023-10-31'), (3, '2023-10-30'),
        (4, '2024-04-30'), (5, '2024-05-01')
),

actual AS ({{ query.sql }}),

expected AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS indicator_status
    FROM VALUES (1, '2024-01-31', 'NOT_RECORDED_IN_PERIOD'),
        (1, '2024-02-29', 'ACHIEVED'),
        (1, '2024-04-30', 'ACHIEVED'),
        (1, '2025-02-01', 'ACHIEVED'),
        (2, '2024-01-31', 'ACHIEVED'),
        (2, '2024-02-29', 'ACHIEVED'),
        (2, '2024-04-30', 'ACHIEVED'),
        (2, '2025-02-01', 'NOT_RECORDED_IN_PERIOD'),
        (3, '2024-01-31', 'NOT_RECORDED_IN_PERIOD'),
        (3, '2024-02-29', 'NOT_RECORDED_IN_PERIOD'),
        (3, '2024-04-30', 'NOT_RECORDED_IN_PERIOD'),
        (3, '2025-02-01', 'NOT_RECORDED_IN_PERIOD'),
        (4, '2024-01-31', 'NOT_RECORDED_IN_PERIOD'),
        (4, '2024-02-29', 'NOT_RECORDED_IN_PERIOD'),
        (4, '2024-04-30', 'ACHIEVED'),
        (4, '2025-02-01', 'ACHIEVED'),
        (5, '2024-01-31', 'NOT_RECORDED_IN_PERIOD'),
        (5, '2024-02-29', 'NOT_RECORDED_IN_PERIOD'),
        (5, '2024-04-30', 'NOT_RECORDED_IN_PERIOD'),
        (5, '2025-02-01', 'ACHIEVED')
),

actual_occurrences AS (
    SELECT person_id, reporting_date, indicator_status, COUNT(*) AS occurrences
    FROM actual
    GROUP BY ALL
),

expected_occurrences AS (
    SELECT person_id, reporting_date, indicator_status, COUNT(*) AS occurrences
    FROM expected
    GROUP BY ALL
),

failures AS (
    (SELECT * FROM actual_occurrences EXCEPT SELECT * FROM expected_occurrences)
    UNION ALL
    (SELECT * FROM expected_occurrences EXCEPT SELECT * FROM actual_occurrences)
)
SELECT COUNT(*) AS failure_count
FROM failures
HAVING COUNT(*) > 0
