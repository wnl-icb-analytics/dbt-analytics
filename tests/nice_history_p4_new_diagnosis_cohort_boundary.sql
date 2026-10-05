{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation_233 = nice_ind233('by_month') %}
{% set calculation_233 = calculation_233 | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation_233 = calculation_233 | replace(ref('int_ckd_profile_by_month') | string, 'synthetic_profile') %}
{% set calculation_234 = nice_ind234('by_month') %}
{% set calculation_234 = calculation_234 | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation_234 = calculation_234 | replace(ref('int_ckd_profile_by_month') | string, 'synthetic_profile') %}

-- IND233 keeps its inclusive 12-month start. IND234 shifts back by 90 days
-- and excludes its start; both open-window outcomes remain outside IND234.
WITH synthetic_keys AS (
    SELECT -9405 AS person_id, column1::DATE AS reporting_date,
        '2025-09-30'::DATE AS diagnosis_date, TRUE AS has_tests
    FROM VALUES ('2026-09-30'), ('2026-10-31')
    UNION ALL
    SELECT column1::NUMBER, '2026-09-30'::DATE, column2::DATE, column3::BOOLEAN
    FROM VALUES
        (-9406, '2025-07-02', TRUE),
        (-9407, '2025-07-03', TRUE),
        (-9408, '2026-07-02', TRUE),
        (-9409, '2026-07-03', TRUE),
        (-9410, '2026-07-03', FALSE),
        (-9411, '2026-07-02', FALSE)
),
synthetic_population AS (
    SELECT
        person_id,
        reporting_date,
        60 AS age,
        '1966-01-01'::DATE AS birth_date_approx,
        'Female'::VARCHAR AS gender,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM synthetic_keys
),
synthetic_profile AS (
    SELECT
        person_id,
        reporting_date,
        diagnosis_date AS ckd_diagnosis_date,
        40::FLOAT AS latest_egfr_value,
        20::FLOAT AS latest_acr_value,
        has_tests AS has_egfr_pair_before_diagnosis,
        DATEADD(day, -29, diagnosis_date) AS second_egfr_before_diagnosis_date,
        has_tests AS has_egfr_within_90_days_of_diagnosis,
        DATEADD(day, -29, diagnosis_date) AS egfr_within_90_days_of_diagnosis_date,
        has_tests AS has_acr_within_90_days_of_diagnosis,
        DATEADD(day, -28, diagnosis_date) AS acr_within_90_days_of_diagnosis_date
    FROM synthetic_keys
),
actual AS (
    SELECT person_id, indicator_id, reporting_date, is_in_numerator FROM ({{ calculation_233 }})
    UNION ALL
    SELECT person_id, indicator_id, reporting_date, is_in_numerator FROM ({{ calculation_234 }})
),
expected AS (
    SELECT column1::NUMBER AS person_id, column2::VARCHAR AS indicator_id,
        column3::DATE AS reporting_date, column4::BOOLEAN AS is_in_numerator
    FROM VALUES
        (-9405, 'IND233', '2026-09-30', TRUE),
        (-9405, 'IND234', '2026-09-30', TRUE),
        (-9405, 'IND234', '2026-10-31', TRUE),
        (-9407, 'IND234', '2026-09-30', TRUE),
        (-9408, 'IND233', '2026-09-30', TRUE),
        (-9408, 'IND234', '2026-09-30', TRUE),
        (-9409, 'IND233', '2026-09-30', TRUE),
        (-9410, 'IND233', '2026-09-30', FALSE),
        (-9411, 'IND233', '2026-09-30', FALSE),
        (-9411, 'IND234', '2026-09-30', FALSE)
),
actual_counts AS (SELECT *, COUNT(*) AS occurrences FROM actual GROUP BY ALL),
expected_counts AS (SELECT *, COUNT(*) AS occurrences FROM expected GROUP BY ALL),
failures AS (
    (SELECT * FROM actual_counts EXCEPT SELECT * FROM expected_counts)
    UNION ALL
    (SELECT * FROM expected_counts EXCEPT SELECT * FROM actual_counts)
)
SELECT COUNT(*) AS failure_count FROM failures HAVING COUNT(*) > 0
