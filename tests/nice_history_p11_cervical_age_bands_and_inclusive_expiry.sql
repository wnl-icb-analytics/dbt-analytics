{{ config(tags=['monthly-full', 'nice-history']) }}

{% set actual_213 = nice_ind213('by_month') %}
{% set actual_213 = actual_213 | replace(nice_reference_population('by_month'), 'SELECT * FROM synthetic_population') %}
{% set actual_213 = actual_213 | replace(nice_ref('int_nice_ltc_population', 'by_month') | string, 'synthetic_ltc') %}
{% set actual_213 = actual_213 | replace(nice_ref('int_nice_cervical_screening_evidence', 'by_month') | string, 'synthetic_screening') %}
{% set actual_214 = nice_ind214('by_month') %}
{% set actual_214 = actual_214 | replace(nice_reference_population('by_month'), 'SELECT * FROM synthetic_population') %}
{% set actual_214 = actual_214 | replace(nice_ref('int_nice_ltc_population', 'by_month') | string, 'synthetic_ltc') %}
{% set actual_214 = actual_214 | replace(nice_ref('int_nice_cervical_screening_evidence', 'by_month') | string, 'synthetic_screening') %}

WITH synthetic_population AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::NUMBER AS age,
        column4::DATE AS birth_date_approx,
        column5::VARCHAR AS gender,
        column6::VARCHAR AS practice_code,
        column7::VARCHAR AS practice_name
    FROM VALUES
        (-11031, '2024-03-31', 25, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11031, '2024-04-30', 25, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11032, '2024-03-31', 49, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11032, '2024-04-30', 49, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11033, '2024-03-31', 50, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11033, '2024-04-30', 50, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11034, '2024-03-31', 64, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11034, '2024-04-30', 64, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11035, '2024-03-31', 24, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11035, '2024-04-30', 24, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11036, '2024-03-31', 65, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11036, '2024-04-30', 65, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11037, '2024-03-31', 30, '1984-01-01', 'Male', 'SYNTHETIC', 'Synthetic practice'),
        (-11037, '2024-04-30', 30, '1984-01-01', 'Male', 'SYNTHETIC', 'Synthetic practice'),
        (-11038, '2024-03-31', 30, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11038, '2024-04-30', 30, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice')
),

synthetic_ltc AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::BOOLEAN AS has_active_smi_diagnosis,
        column4::DATE AS earliest_smi_diagnosis_date,
        column5::DATE AS earliest_cvd_diagnosis_date,
        column6::DATE AS earliest_diabetes_diagnosis_date
    FROM VALUES
        (-11031, '2024-03-31', TRUE, '2020-01-01', NULL, NULL),
        (-11031, '2024-04-30', TRUE, '2020-01-01', NULL, NULL),
        (-11032, '2024-03-31', TRUE, '2020-01-01', NULL, NULL),
        (-11032, '2024-04-30', TRUE, '2020-01-01', NULL, NULL),
        (-11033, '2024-03-31', TRUE, '2020-01-01', NULL, NULL),
        (-11033, '2024-04-30', TRUE, '2020-01-01', NULL, NULL),
        (-11034, '2024-03-31', TRUE, '2020-01-01', NULL, NULL),
        (-11034, '2024-04-30', TRUE, '2020-01-01', NULL, NULL),
        (-11035, '2024-03-31', TRUE, '2020-01-01', NULL, NULL),
        (-11035, '2024-04-30', TRUE, '2020-01-01', NULL, NULL),
        (-11036, '2024-03-31', TRUE, '2020-01-01', NULL, NULL),
        (-11036, '2024-04-30', TRUE, '2020-01-01', NULL, NULL),
        (-11037, '2024-03-31', TRUE, '2020-01-01', NULL, NULL),
        (-11037, '2024-04-30', TRUE, '2020-01-01', NULL, NULL),
        (-11038, '2024-03-31', FALSE, '2020-01-01', NULL, NULL),
        (-11038, '2024-04-30', FALSE, '2020-01-01', NULL, NULL)
),

synthetic_screening AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::TIMESTAMP_NTZ AS latest_completed_date,
        column4::TIMESTAMP_NTZ AS first_cervix_removal_date,
        column5::BOOLEAN AS is_currently_pregnant
    FROM VALUES
        (-11031, '2024-03-31', '2020-09-30', '2020-01-01', TRUE),
        (-11031, '2024-04-30', '2020-09-30', '2020-01-01', TRUE),
        (-11032, '2024-03-31', '2020-09-30', '2020-01-01', TRUE),
        (-11032, '2024-04-30', '2020-09-30', '2020-01-01', TRUE),
        (-11033, '2024-03-31', '2018-09-30', '2020-01-01', TRUE),
        (-11033, '2024-04-30', '2018-09-30', '2020-01-01', TRUE),
        (-11034, '2024-03-31', '2018-09-30', '2020-01-01', TRUE),
        (-11034, '2024-04-30', '2018-09-30', '2020-01-01', TRUE),
        (-11035, '2024-03-31', '2020-09-30', '2020-01-01', TRUE),
        (-11035, '2024-04-30', '2020-09-30', '2020-01-01', TRUE),
        (-11036, '2024-03-31', '2018-09-30', '2020-01-01', TRUE),
        (-11036, '2024-04-30', '2018-09-30', '2020-01-01', TRUE),
        (-11037, '2024-03-31', '2020-09-30', '2020-01-01', TRUE),
        (-11037, '2024-04-30', '2020-09-30', '2020-01-01', TRUE),
        (-11038, '2024-03-31', '2020-09-30', '2020-01-01', TRUE),
        (-11038, '2024-04-30', '2020-09-30', '2020-01-01', TRUE)
),

actual_213 AS (
    {{ actual_213 }}
),

actual_214 AS (
    {{ actual_214 }}
),

actual AS (
SELECT person_id, reporting_date, indicator_id, is_in_numerator, latest_record_date FROM actual_213
UNION ALL
SELECT person_id, reporting_date, indicator_id, is_in_numerator, latest_record_date FROM actual_214
),

expected AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS indicator_id,
        column4::BOOLEAN AS is_in_numerator,
        column5::DATE AS latest_record_date
    FROM VALUES
        (-11031, '2024-03-31', 'IND213', TRUE, '2020-09-30'),
        (-11031, '2024-04-30', 'IND213', FALSE, NULL),
        (-11032, '2024-03-31', 'IND213', TRUE, '2020-09-30'),
        (-11032, '2024-04-30', 'IND213', FALSE, NULL),
        (-11033, '2024-03-31', 'IND214', TRUE, '2018-09-30'),
        (-11033, '2024-04-30', 'IND214', FALSE, NULL),
        (-11034, '2024-03-31', 'IND214', TRUE, '2018-09-30'),
        (-11034, '2024-04-30', 'IND214', FALSE, NULL)
),

differences AS (
    (SELECT * FROM actual EXCEPT SELECT * FROM expected)
    UNION ALL
    (SELECT * FROM expected EXCEPT SELECT * FROM actual)
)
SELECT COUNT(*) AS rows_total
FROM differences
HAVING COUNT(*) <> 0
    OR (SELECT COUNT(*) FROM actual) <> (SELECT COUNT(*) FROM expected)
