{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = nice_ind85('by_month') %}
{% set calculation = calculation | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation = calculation | replace(ref('int_nice_ltc_population_by_month') | string, 'synthetic_ltc') %}
{% set calculation = calculation | replace(ref('int_nice_cervical_screening_evidence_by_month') | string, 'synthetic_cervical') %}

WITH synthetic_population AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::NUMBER AS age,
        column4::VARCHAR AS gender,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name,
        '1984-01-01'::DATE AS birth_date_approx
    FROM VALUES
        (-10141, '2024-04-30', 25, 'Female'),
        (-10142, '2024-04-30', 64, 'Female'),
        (-10142, '2024-05-31', 65, 'Female'),
        (-10143, '2024-04-30', 24, 'Female'),
        (-10144, '2024-04-30', 65, 'Female'),
        (-10145, '2024-04-30', 40, 'Male'),
        (-10146, '2024-04-30', 40, 'Female')
),
synthetic_ltc AS (
    SELECT
        person_id,
        reporting_date,
        person_id <> -10146 AS has_active_smi_diagnosis
    FROM synthetic_population
),
synthetic_cervical AS (
    SELECT
        person_id,
        reporting_date,
        '2024-04-01'::DATE AS latest_completed_date,
        TRUE AS is_currently_pregnant,
        '2020-01-01'::DATE AS first_cervix_removal_date
    FROM synthetic_population
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS failure_count
FROM actual
-- IND85 retains its SMI population without the general screening exclusions.
HAVING COUNT(*) <> 2
    OR COUNT_IF(age IN (25, 64) AND reporting_date = '2024-04-30'
        AND latest_record_date = '2024-04-01' AND is_in_numerator) <> 2
