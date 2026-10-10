{{ config(tags=['monthly-full', 'nice-history']) }}

{% set query = namespace(sql=nice_ind247('by_month')) %}
{% set query.sql = query.sql | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set query.sql = query.sql | replace(nice_ref('int_atrial_fibrillation_profile', 'by_month') | string, 'synthetic_profile') %}

WITH synthetic_population AS (
    SELECT
        people.column1::NUMBER AS person_id,
        dates.column1::DATE AS reporting_date,
        50 AS age,
        'SYNTHETIC'::VARCHAR AS practice_code,
        'Synthetic practice'::VARCHAR AS practice_name
    FROM VALUES (1), (2), (3), (4), (5) AS people
    CROSS JOIN (SELECT column1 FROM VALUES ('2024-01-31'), ('2024-02-01')) AS dates
),

synthetic_profile AS (
    SELECT
        population.person_id AS person_id,
        NULL::DATE AS earliest_af_diagnosis_date,
        3::FLOAT AS latest_chadsvasc_score,
        population.reporting_date AS latest_chadsvasc_date,
        NULL::FLOAT AS latest_chads2_score,
        NULL::DATE AS latest_chads2_date,
        NULL::DATE AS latest_stroke_risk_score_date,
        NULL::FLOAT AS latest_stroke_risk_score,
        NULL::FLOAT AS max_stroke_risk_score_ever,
        NULL::FLOAT AS max_stroke_risk_score_before_period,
        '2023-07-31'::DATE AS latest_anticoagulant_order_date,
        NULL::VARCHAR AS latest_anticoagulant_type,
        IFF(person_id IN (1, 5), '2023-07-31'::DATE, NULL::DATE) AS latest_doac_order_date,
        IFF(person_id IN (2, 3, 4), '2023-07-31'::DATE, NULL::DATE) AS latest_vka_order_date,
        FALSE AS has_anticoagulant_adverse_reaction,
        FALSE AS has_anticoagulant_persisting_contraindication,
        NULL::DATE AS latest_anticoagulant_contraindicated_date,
        NULL::DATE AS latest_anticoagulant_declined_date,
        person_id IN (3, 5) AS is_doac_ineligible,
        person_id = 2 AS has_doac_exception,
        NULL::DATE AS latest_anticoagulant_review_date,
        population.reporting_date AS reporting_date
    FROM synthetic_population AS population
),

actual AS ({{ query.sql }}),

expected AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS indicator_status
    FROM VALUES (1, '2024-01-31', 'ACHIEVED'),
        (1, '2024-02-01', 'NOT_TREATED_IN_PERIOD'),
        (2, '2024-01-31', 'ACHIEVED'),
        (2, '2024-02-01', 'NOT_TREATED_IN_PERIOD'),
        (3, '2024-01-31', 'ACHIEVED'),
        (3, '2024-02-01', 'NOT_TREATED_IN_PERIOD'),
        (4, '2024-01-31', 'VKA_WITHOUT_DOAC_EXCEPTION'),
        (4, '2024-02-01', 'NOT_TREATED_IN_PERIOD'),
        (5, '2024-01-31', 'DOAC_WHERE_VKA_INDICATED'),
        (5, '2024-02-01', 'NOT_TREATED_IN_PERIOD')
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
