{{ config(tags=['monthly-full', 'nice-history']) }}

{% set actual_172 = nice_ind172('by_month') %}
{% set actual_172 = actual_172 | replace(nice_reference_population('by_month'), 'SELECT * FROM synthetic_population') %}
{% set actual_172 = actual_172 | replace(nice_register('NDH', 'by_month'), 'SELECT * FROM synthetic_ndh') %}
{% set actual_172 = actual_172 | replace(nice_ref('int_nice_hba1c_evidence', 'by_month') | string, 'synthetic_hba') %}
{% set actual_172 = actual_172 | replace(ref('int_blood_glucose_all') | string, 'synthetic_glucose') %}

WITH synthetic_keys AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::NUMBER AS age
    FROM VALUES
        (606, '2024-01-31', 18),
        (606, '2024-02-29', 18),
        (607, '2024-01-31', 17),
        (608, '2024-01-31', 18)
),

synthetic_population AS (
    SELECT
        person_id,
        reporting_date,
        age,
        '1980-01-01'::DATE AS birth_date_approx,
        'Female'::VARCHAR AS gender,
        'SYNTHETIC'::VARCHAR AS practice_code,
        'Synthetic practice'::VARCHAR AS practice_name
    FROM synthetic_keys
),

synthetic_ndh AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::BOOLEAN AS has_unresolved_diabetes
    FROM VALUES
        (606, '2024-01-31', FALSE),
        (606, '2024-02-29', FALSE),
        (607, '2024-01-31', FALSE),
        (608, '2024-01-31', TRUE)
),

synthetic_hba AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS latest_hba1c_observation_id,
        column4::DATE AS latest_hba1c_date,
        column5::NUMBER AS latest_hba1c_value,
        column6::BOOLEAN AS is_latest_hba1c_valid,
        column7::DATE AS latest_fructosamine_date,
        column8::DATE AS latest_dmmax_date
    FROM VALUES
        (606, '2024-01-31', NULL, NULL, NULL, FALSE, NULL, NULL),
        (606, '2024-02-29', NULL, NULL, NULL, FALSE, NULL, NULL)
),

synthetic_glucose AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS clinical_effective_date,
        column3::VARCHAR AS source_cluster_id
    FROM VALUES
        (606, '2023-01-31', 'FASPLASGLUC_COD'),
        (606, '2024-01-30', 'OTHER'),
        (606, '2024-03-01', 'FASPLASGLUC_COD')
),

actual_172 AS ({{ actual_172 }})

SELECT COUNT(*) AS failure_count
FROM actual_172
HAVING COUNT(*) <> 2
    OR COUNT_IF(reporting_date = '2024-01-31' AND latest_record_date = '2023-01-31'
        AND is_in_numerator) <> 1
    OR COUNT_IF(reporting_date = '2024-02-29' AND latest_record_date IS NULL
        AND NOT is_in_numerator) <> 1
