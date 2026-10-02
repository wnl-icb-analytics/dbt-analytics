{{ config(tags=['monthly-full', 'nice-history']) }}

{% set actual_248 = nice_ind248('by_month') %}
{% set actual_248 = actual_248 | replace(nice_reference_population('by_month'), 'SELECT * FROM synthetic_population') %}
{% set actual_248 = actual_248 | replace(nice_ref('int_nice_ltc_population', 'by_month') | string, 'synthetic_ltc') %}
{% set actual_248 = actual_248 | replace(nice_ref('int_nice_smoking_evidence', 'by_month') | string, 'synthetic_smoking') %}
{% set actual_248 = actual_248 | replace(nice_ref('int_nice_physical_health_evidence', 'by_month') | string, 'synthetic_physical') %}

WITH synthetic_population AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::NUMBER AS age,
        column4::DATE AS birth_date_approx,
        column5::VARCHAR AS gender,
        column6::VARCHAR AS practice_code,
        column7::VARCHAR AS practice_name
    FROM VALUES
        (-11041, '2024-03-31', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11041, '2024-04-30', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11042, '2024-03-31', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11042, '2024-04-30', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11043, '2024-03-31', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11043, '2024-04-30', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice')
),

synthetic_ltc AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::BOOLEAN AS has_active_smi_diagnosis,
        column4::DATE AS earliest_smi_diagnosis_date,
        column5::DATE AS earliest_cvd_diagnosis_date,
        column6::DATE AS earliest_diabetes_diagnosis_date
    FROM VALUES
        (-11041, '2024-03-31', TRUE, '2020-01-01', NULL, NULL),
        (-11041, '2024-04-30', TRUE, '2020-01-01', NULL, NULL),
        (-11042, '2024-03-31', TRUE, '2020-01-01', NULL, NULL),
        (-11042, '2024-04-30', TRUE, '2020-01-01', NULL, NULL),
        (-11043, '2024-03-31', TRUE, '2020-01-01', NULL, NULL),
        (-11043, '2024-04-30', FALSE, '2020-01-01', NULL, NULL)
),

synthetic_smoking AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS latest_smoking_status,
        column4::DATE AS latest_smoking_status_date,
        column5::DATE AS latest_never_smoked_date,
        column6::DATE AS latest_smoking_intervention_date
    FROM VALUES
        (-11041, '2024-03-31', 'Ex-Smoker', '2023-03-31', NULL, NULL),
        (-11041, '2024-04-30', 'Ex-Smoker', '2023-03-31', NULL, NULL),
        (-11042, '2024-03-31', 'Ex-Smoker', NULL, NULL, NULL),
        (-11042, '2024-04-30', 'Ex-Smoker', NULL, NULL, NULL),
        (-11043, '2024-03-31', 'Ex-Smoker', '2024-03-01', NULL, NULL),
        (-11043, '2024-04-30', 'Ex-Smoker', '2024-03-01', NULL, NULL)
),

synthetic_physical AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::DATE AS latest_blood_pressure_date,
        column4::DATE AS latest_bmi_date,
        column5::DATE AS latest_alcohol_record_date,
        column6::DATE AS latest_lipid_date,
        column7::DATE AS latest_glucose_or_hba1c_date,
        column8::DATE AS latest_cholesterol_hdl_ratio_date
    FROM VALUES
        (-11041, '2024-03-31', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01'),
        (-11041, '2024-04-30', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01'),
        (-11042, '2024-03-31', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01'),
        (-11042, '2024-04-30', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01'),
        (-11043, '2024-03-31', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01'),
        (-11043, '2024-04-30', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01', '2024-03-01')
),

actual_248 AS (
    {{ actual_248 }}
),

actual AS (
SELECT person_id, reporting_date, indicator_id, is_in_numerator, latest_record_date FROM actual_248
),

expected AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS indicator_id,
        column4::BOOLEAN AS is_in_numerator,
        column5::DATE AS latest_record_date
    FROM VALUES
        (-11041, '2024-03-31', 'IND248', TRUE, '2024-03-01'),
        (-11041, '2024-04-30', 'IND248', FALSE, NULL),
        (-11042, '2024-03-31', 'IND248', FALSE, NULL),
        (-11042, '2024-04-30', 'IND248', FALSE, NULL),
        (-11043, '2024-03-31', 'IND248', TRUE, '2024-03-01')
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
    OR (SELECT COUNT_IF(checks_met_count = 6) FROM actual_248) <> 2
    OR (SELECT COUNT_IF(checks_met_count = 5) FROM actual_248) <> 3
