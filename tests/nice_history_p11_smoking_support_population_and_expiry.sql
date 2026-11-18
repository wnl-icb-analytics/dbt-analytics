{{ config(tags=['monthly-full', 'nice-history']) }}

{% set actual_155 = nice_ind155('by_month') %}
{% set actual_155 = actual_155 | replace(nice_reference_population('by_month'), 'SELECT * FROM synthetic_population') %}
{% set actual_155 = actual_155 | replace(nice_ref('int_nice_ltc_population', 'by_month') | string, 'synthetic_ltc') %}
{% set actual_155 = actual_155 | replace(nice_ref('int_nice_smoking_evidence', 'by_month') | string, 'synthetic_smoking') %}

WITH synthetic_population AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::NUMBER AS age,
        column4::DATE AS birth_date_approx,
        column5::VARCHAR AS gender,
        column6::VARCHAR AS practice_code,
        column7::VARCHAR AS practice_name
    FROM VALUES
        (-11011, '2024-03-31', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11011, '2024-04-30', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11012, '2024-03-31', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11012, '2024-04-30', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11013, '2024-03-31', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11013, '2024-04-30', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice')
),

synthetic_ltc AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::BOOLEAN AS has_active_smi_diagnosis,
        column4::DATE AS earliest_smi_diagnosis_date,
        column5::DATE AS earliest_cvd_diagnosis_date,
        column6::DATE AS earliest_diabetes_diagnosis_date
    FROM VALUES
        (-11011, '2024-03-31', TRUE, '2020-01-01', NULL, NULL),
        (-11011, '2024-04-30', TRUE, '2020-01-01', NULL, NULL),
        (-11012, '2024-03-31', FALSE, '2020-01-01', NULL, NULL),
        (-11012, '2024-04-30', FALSE, '2020-01-01', NULL, NULL),
        (-11013, '2024-03-31', TRUE, '2020-01-01', NULL, NULL),
        (-11013, '2024-04-30', TRUE, '2020-01-01', NULL, NULL)
),

synthetic_smoking AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS latest_smoking_status,
        column4::DATE AS latest_smoking_status_date,
        column5::DATE AS latest_never_smoked_date,
        column6::DATE AS latest_smoking_intervention_date
    FROM VALUES
        (-11011, '2024-03-31', 'Current Smoker', '2024-03-31', NULL, '2023-03-31'),
        (-11011, '2024-04-30', 'Ex-Smoker', '2024-04-30', NULL, '2023-03-31'),
        (-11012, '2024-03-31', 'Current Smoker', '2024-03-31', NULL, '2023-03-31'),
        (-11012, '2024-04-30', 'Current Smoker', '2024-04-30', NULL, '2023-03-31'),
        (-11013, '2024-03-31', 'Current Smoker', '2024-03-31', NULL, '2023-03-31'),
        (-11013, '2024-04-30', 'Current Smoker', '2024-04-30', NULL, '2023-03-31')
),

actual_155 AS (
    {{ actual_155 }}
),

actual AS (
SELECT person_id, reporting_date, indicator_id, is_in_numerator, latest_record_date FROM actual_155
),

expected AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS indicator_id,
        column4::BOOLEAN AS is_in_numerator,
        column5::DATE AS latest_record_date
    FROM VALUES
        (-11011, '2024-03-31', 'IND155', TRUE, '2023-03-31'),
        (-11013, '2024-03-31', 'IND155', TRUE, '2023-03-31'),
        (-11013, '2024-04-30', 'IND155', FALSE, NULL)
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
