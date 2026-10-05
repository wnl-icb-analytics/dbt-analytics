{{ config(tags=['monthly-full', 'nice-history']) }}

{% set actual_158 = nice_ind158('by_month') %}
{% set actual_158 = actual_158 | replace(nice_reference_population('by_month'), 'SELECT * FROM synthetic_population') %}
{% set actual_158 = actual_158 | replace(nice_ref('int_nice_ltc_population', 'by_month') | string, 'synthetic_ltc') %}
{% set actual_158 = actual_158 | replace(nice_ref('int_nice_physical_health_evidence', 'by_month') | string, 'synthetic_physical') %}
{% set actual_159 = nice_ind159('by_month') %}
{% set actual_159 = actual_159 | replace(nice_reference_population('by_month'), 'SELECT * FROM synthetic_population') %}
{% set actual_159 = actual_159 | replace(nice_ref('int_nice_ltc_population', 'by_month') | string, 'synthetic_ltc') %}
{% set actual_159 = actual_159 | replace(nice_ref('int_nice_physical_health_evidence', 'by_month') | string, 'synthetic_physical') %}

WITH synthetic_population AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::NUMBER AS age,
        column4::DATE AS birth_date_approx,
        column5::VARCHAR AS gender,
        column6::VARCHAR AS practice_code,
        column7::VARCHAR AS practice_name
    FROM VALUES
        (-11021, '2024-03-31', 18, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11021, '2024-04-30', 18, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11022, '2024-03-31', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11022, '2024-04-30', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11023, '2024-03-31', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11023, '2024-04-30', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11024, '2024-03-31', 17, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11024, '2024-04-30', 17, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11025, '2024-03-31', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11025, '2024-04-30', 40, '1984-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11026, '2024-03-31', 18, '2006-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice'),
        (-11026, '2024-04-30', 18, '2006-01-01', 'Female', 'SYNTHETIC', 'Synthetic practice')
),

synthetic_ltc AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::BOOLEAN AS has_active_smi_diagnosis,
        column4::DATE AS earliest_smi_diagnosis_date,
        column5::DATE AS earliest_cvd_diagnosis_date,
        column6::DATE AS earliest_diabetes_diagnosis_date
    FROM VALUES
        (-11021, '2024-03-31', TRUE, '2020-01-01', '2023-03-31', '2023-03-31'),
        (-11021, '2024-04-30', TRUE, '2020-01-01', '2023-03-31', '2023-03-31'),
        (-11022, '2024-03-31', TRUE, '2020-01-01', '2023-03-31', '2023-03-31'),
        (-11022, '2024-04-30', TRUE, '2020-01-01', '2023-03-31', '2023-03-31'),
        (-11023, '2024-03-31', FALSE, '2020-01-01', '2023-03-31', '2023-03-31'),
        (-11023, '2024-04-30', FALSE, '2020-01-01', '2023-03-31', '2023-03-31'),
        (-11024, '2024-03-31', TRUE, '2020-01-01', '2023-03-31', '2023-03-31'),
        (-11024, '2024-04-30', TRUE, '2020-01-01', '2023-03-31', '2023-03-31'),
        (-11025, '2024-03-31', TRUE, '2020-01-01', NULL, NULL),
        (-11025, '2024-04-30', TRUE, '2020-01-01', NULL, NULL),
        (-11026, '2024-03-31', TRUE, '2020-01-01', NULL, NULL),
        (-11026, '2024-04-30', TRUE, '2020-01-01', NULL, NULL)
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
        (-11021, '2024-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31'),
        (-11021, '2024-04-30', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31'),
        (-11022, '2024-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31'),
        (-11022, '2024-04-30', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31'),
        (-11023, '2024-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31'),
        (-11023, '2024-04-30', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31'),
        (-11024, '2024-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31'),
        (-11024, '2024-04-30', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31'),
        (-11025, '2024-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31'),
        (-11025, '2024-04-30', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31'),
        (-11026, '2024-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31'),
        (-11026, '2024-04-30', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31', '2023-03-31')
),

actual_158 AS (
    {{ actual_158 }}
),

actual_159 AS (
    {{ actual_159 }}
),

actual AS (
SELECT person_id, reporting_date, indicator_id, is_in_numerator, latest_record_date FROM actual_158
UNION ALL
SELECT person_id, reporting_date, indicator_id, is_in_numerator, latest_record_date FROM actual_159
),

expected AS (
    SELECT column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS indicator_id,
        column4::BOOLEAN AS is_in_numerator,
        column5::DATE AS latest_record_date
    FROM VALUES
        (-11021, '2024-03-31', 'IND158', TRUE, '2023-03-31'),
        (-11021, '2024-03-31', 'IND159', TRUE, '2023-03-31'),
        (-11022, '2024-03-31', 'IND158', TRUE, '2023-03-31'),
        (-11022, '2024-03-31', 'IND159', TRUE, '2023-03-31'),
        (-11025, '2024-03-31', 'IND158', TRUE, '2023-03-31'),
        (-11025, '2024-03-31', 'IND159', TRUE, '2023-03-31'),
        (-11025, '2024-04-30', 'IND158', FALSE, NULL),
        (-11025, '2024-04-30', 'IND159', FALSE, NULL),
        (-11026, '2024-03-31', 'IND158', TRUE, '2023-03-31'),
        (-11026, '2024-03-31', 'IND159', TRUE, '2023-03-31'),
        (-11026, '2024-04-30', 'IND158', FALSE, NULL),
        (-11026, '2024-04-30', 'IND159', FALSE, NULL)
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
