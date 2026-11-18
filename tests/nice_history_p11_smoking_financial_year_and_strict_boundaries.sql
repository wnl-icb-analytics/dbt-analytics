{{ config(tags=['monthly-full', 'nice-history']) }}

{% set actual_154 = nice_ind154('by_month') %}
{% set actual_154 = actual_154 | replace(nice_reference_population('by_month'), 'SELECT * FROM synthetic_population') %}
{% set actual_154 = actual_154 | replace(nice_ref('int_nice_ltc_population', 'by_month') | string, 'synthetic_ltc') %}
{% set actual_154 = actual_154 | replace(nice_ref('int_nice_smoking_evidence', 'by_month') | string, 'synthetic_smoking') %}

WITH synthetic_population AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::NUMBER AS age,
        column4::DATE AS birth_date_approx,
        column5::VARCHAR AS gender,
        column6::VARCHAR AS practice_code,
        column7::VARCHAR AS practice_name
    FROM VALUES
        (-11001, '2024-03-31', 25, '1998-04-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11001, '2024-04-30', 25, '1998-04-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11002, '2024-03-31', 26, '1998-03-30', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11002, '2024-04-30', 26, '1998-03-30', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11003, '2024-03-31', 28, '1996-03-30', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11003, '2024-04-30', 28, '1996-03-30', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11004, '2024-03-31', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11004, '2024-04-30', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11005, '2024-03-31', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11005, '2024-04-30', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice')
),

synthetic_ltc AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::BOOLEAN AS has_active_smi_diagnosis,
        column4::DATE AS earliest_smi_diagnosis_date,
        column5::DATE AS earliest_cvd_diagnosis_date,
        column6::DATE AS earliest_diabetes_diagnosis_date
    FROM VALUES
        (-11001, '2024-03-31', TRUE, '2022-01-01', NULL, NULL),
        (-11001, '2024-04-30', TRUE, '2022-01-01', NULL, NULL),
        (-11002, '2024-03-31', TRUE, '2022-01-01', NULL, NULL),
        (-11002, '2024-04-30', TRUE, '2022-01-01', NULL, NULL),
        (-11003, '2024-03-31', TRUE, '2022-01-01', NULL, NULL),
        (-11003, '2024-04-30', TRUE, '2022-01-01', NULL, NULL),
        (-11004, '2024-03-31', TRUE, '2022-01-01', NULL, NULL),
        (-11004, '2024-04-30', TRUE, '2022-01-01', NULL, NULL),
        (-11005, '2024-03-31', TRUE, '2022-01-01', NULL, NULL),
        (-11005, '2024-04-30', TRUE, '2022-01-01', NULL, NULL)
),

synthetic_smoking AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS latest_smoking_status,
        column4::DATE AS latest_smoking_status_date,
        column5::DATE AS latest_never_smoked_date,
        column6::DATE AS latest_smoking_intervention_date,
        FALSE AS is_ex_smoker_covered
    FROM VALUES
        (-11001, '2024-03-31', 'Never Smoked', '2023-04-02', '2023-04-02', NULL),
        (-11001, '2024-04-30', 'Never Smoked', '2023-04-02', '2023-04-02', NULL),
        (-11002, '2024-03-31', 'Never Smoked', '2023-03-30', '2023-03-30', NULL),
        (-11002, '2024-04-30', 'Never Smoked', '2023-03-30', '2023-03-30', NULL),
        (-11003, '2024-03-31', 'Never Smoked', '2022-01-01', '2022-01-01', NULL),
        (-11003, '2024-04-30', 'Never Smoked', '2022-01-01', '2022-01-01', NULL),
        (-11004, '2024-03-31', 'Current Smoker', '2023-03-31', NULL, NULL),
        (-11004, '2024-04-30', 'Current Smoker', '2023-03-31', NULL, NULL),
        (-11005, '2024-03-31', 'Ex-Smoker', '2022-07-01', '2022-01-02', NULL),
        (-11005, '2024-04-30', 'Ex-Smoker', '2022-07-01', '2022-01-02', NULL)
),

actual_154 AS (
    {{ actual_154 }}
),

actual AS (
SELECT person_id, reporting_date, indicator_id, is_in_numerator, latest_record_date FROM actual_154
),

expected AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS indicator_id,
        column4::BOOLEAN AS is_in_numerator,
        column5::DATE AS latest_record_date
    FROM VALUES
        (-11001, '2024-03-31', 'IND154', TRUE, '2023-04-02'),
        (-11001, '2024-04-30', 'IND154', TRUE, NULL),
        (-11002, '2024-03-31', 'IND154', FALSE, NULL),
        (-11002, '2024-04-30', 'IND154', FALSE, NULL),
        (-11003, '2024-03-31', 'IND154', FALSE, NULL),
        (-11003, '2024-04-30', 'IND154', FALSE, NULL),
        (-11004, '2024-03-31', 'IND154', TRUE, '2023-03-31'),
        (-11004, '2024-04-30', 'IND154', FALSE, NULL),
        (-11005, '2024-03-31', 'IND154', FALSE, NULL),
        (-11005, '2024-04-30', 'IND154', FALSE, NULL)
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
