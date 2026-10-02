{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation82 = nice_ind82('by_month') %}
{% set calculation82 = calculation82 | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation82 = calculation82 | replace(ref('int_nice_ltc_population_by_month') | string, 'synthetic_ltc') %}
{% set calculation82 = calculation82 | replace(ref('int_nice_physical_health_evidence_by_month') | string, 'synthetic_physical') %}
{% set calculation82 = calculation82 | replace(ref('int_nice_cervical_screening_evidence_by_month') | string, 'synthetic_cervical') %}
{% set calculation83 = nice_ind83('by_month') %}
{% set calculation83 = calculation83 | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation83 = calculation83 | replace(ref('int_nice_ltc_population_by_month') | string, 'synthetic_ltc') %}
{% set calculation83 = calculation83 | replace(ref('int_nice_physical_health_evidence_by_month') | string, 'synthetic_physical') %}
{% set calculation83 = calculation83 | replace(ref('int_nice_cervical_screening_evidence_by_month') | string, 'synthetic_cervical') %}
{% set calculation84 = nice_ind84('by_month') %}
{% set calculation84 = calculation84 | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation84 = calculation84 | replace(ref('int_nice_ltc_population_by_month') | string, 'synthetic_ltc') %}
{% set calculation84 = calculation84 | replace(ref('int_nice_physical_health_evidence_by_month') | string, 'synthetic_physical') %}
{% set calculation84 = calculation84 | replace(ref('int_nice_cervical_screening_evidence_by_month') | string, 'synthetic_cervical') %}
{% set calculation85 = nice_ind85('by_month') %}
{% set calculation85 = calculation85 | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation85 = calculation85 | replace(ref('int_nice_ltc_population_by_month') | string, 'synthetic_ltc') %}
{% set calculation85 = calculation85 | replace(ref('int_nice_physical_health_evidence_by_month') | string, 'synthetic_physical') %}
{% set calculation85 = calculation85 | replace(ref('int_nice_cervical_screening_evidence_by_month') | string, 'synthetic_cervical') %}
WITH synthetic_population AS (
    SELECT -10100::NUMBER AS person_id, column1::DATE AS reporting_date,
        40 AS age, 'Female' AS gender, 'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name, '1984-01-01'::DATE AS birth_date_approx
    FROM VALUES ('2024-04-30'), ('2024-05-31')
),
synthetic_ltc AS (
    SELECT person_id, reporting_date, TRUE AS has_active_smi_diagnosis
    FROM synthetic_population
),
synthetic_physical AS (
    SELECT person_id, reporting_date, '2023-01-30'::DATE AS latest_alcohol_record_date,
        '2023-01-30'::DATE AS latest_bmi_date,
        '2023-01-30'::DATE AS latest_blood_pressure_date
    FROM synthetic_population
),
synthetic_cervical AS (
    SELECT person_id, reporting_date, '2019-04-30'::DATE AS latest_completed_date
    FROM synthetic_population
),
actual82 AS ({{ calculation82 }}),
actual83 AS ({{ calculation83 }}),
actual84 AS ({{ calculation84 }}),
actual85 AS ({{ calculation85 }})
SELECT COUNT(*) AS failure_count
FROM (
    SELECT indicator_id, reporting_date, latest_record_date, is_in_numerator, indicator_status FROM actual82
    UNION ALL
    SELECT indicator_id, reporting_date, latest_record_date, is_in_numerator, indicator_status FROM actual83
    UNION ALL
    SELECT indicator_id, reporting_date, latest_record_date, is_in_numerator, indicator_status FROM actual84
    UNION ALL
    SELECT indicator_id, reporting_date, latest_record_date, is_in_numerator, indicator_status FROM actual85
)
HAVING COUNT(*) <> 8
    OR COUNT_IF(reporting_date = '2024-04-30' AND is_in_numerator
        AND indicator_status = 'ACHIEVED' AND latest_record_date IS NOT NULL) <> 4
    OR COUNT_IF(reporting_date = '2024-05-31' AND NOT is_in_numerator
        AND indicator_status = 'NOT_RECORDED_IN_PERIOD' AND latest_record_date IS NULL) <> 4
