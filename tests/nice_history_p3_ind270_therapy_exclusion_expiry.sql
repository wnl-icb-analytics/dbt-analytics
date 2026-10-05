{{ config(tags=['monthly-full', 'nice-history']) }}

{% set query = namespace(sql=nice_ind270('by_month')) %}
{% set query.sql = query.sql | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set query.sql = query.sql | replace(nice_ref('int_cvd_risk_profile', 'by_month') | string, 'synthetic_profile') %}

WITH synthetic_population AS (
    SELECT
        people.column1::NUMBER AS person_id,
        dates.column1::DATE AS reporting_date,
        50 AS age,
        'SYNTHETIC'::VARCHAR AS practice_code,
        'Synthetic practice'::VARCHAR AS practice_name
    FROM VALUES (1) AS people
    CROSS JOIN (SELECT column1 FROM VALUES ('2024-01-31'), ('2024-02-01')) AS dates
),

synthetic_profile AS (
    SELECT
        population.person_id AS person_id,
        5::FLOAT AS latest_risk_score,
        population.reporting_date AS latest_risk_score_date,
        NULL::FLOAT AS latest_risk_score_type,
        NULL::FLOAT AS max_risk_score_ever,
        NULL::FLOAT AS max_risk_score_12m,
        NULL::FLOAT AS min_risk_score_36m,
        population.reporting_date AS latest_risk_assessment_date,
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
        TRUE AS has_type2_diabetes,
        NULL::DATE AS earliest_type2_diabetes_date,
        FALSE AS has_hypertension,
        NULL::DATE AS earliest_hypertension_date,
        NULL::VARCHAR AS latest_frailty_severity,
        '2023-07-31'::DATE AS latest_lipid_lowering_order_date,
        '2023-07-31'::DATE AS latest_statin_order_date,
        TRUE AS is_current_smoker,
        FALSE AS has_obesity,
        NULL::VARCHAR AS latest_total_cholesterol,
        NULL::DATE AS latest_total_cholesterol_date,
        population.reporting_date AS reporting_date
    FROM synthetic_population AS population
),

actual AS ({{ query.sql }}),

expected AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS indicator_status
    FROM VALUES (1, '2024-02-01', 'ACHIEVED')
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
