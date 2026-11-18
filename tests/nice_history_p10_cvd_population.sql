{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation150 = nice_ind150('by_month') %}
{% set calculation150 = calculation150 | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation150 = calculation150 | replace(ref('int_nice_ltc_population_by_month') | string, 'synthetic_ltc') %}
{% set calculation150 = calculation150 | replace(ref('int_cvd_risk_profile_by_month') | string, 'synthetic_cvd') %}
WITH synthetic_population AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS reporting_date,
        column3::NUMBER AS age, 'Female' AS gender, 'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name, '1984-01-01'::DATE AS birth_date_approx
    FROM VALUES (-10121,'2024-04-30',25), (-10121,'2024-05-31',25),
        (-10122,'2024-04-30',84), (-10123,'2024-04-30',24),
        (-10124,'2024-04-30',85), (-10125,'2024-04-30',40),
        (-10126,'2024-04-30',40), (-10127,'2024-04-30',40),
        (-10128,'2024-04-30',40), (-10129,'2024-04-30',40)
),
synthetic_ltc AS (
    SELECT person_id, reporting_date, person_id <> -10129 AS has_active_smi_diagnosis,
        IFF(person_id = -10128, '2024-04-01'::DATE, NULL) AS earliest_cvd_diagnosis_date
    FROM synthetic_population
),
synthetic_cvd AS (
    SELECT person_id, reporting_date,
        person_id = -10125 OR reporting_date = '2024-05-31' AS has_ckd,
        person_id = -10126 AS has_familial_hypercholesterolaemia,
        person_id = -10127 AS has_type1_diabetes,
        '2023-04-30'::DATE AS latest_risk_assessment_date
    FROM synthetic_population
),
actual150 AS ({{ calculation150 }})
SELECT COUNT(*) AS failure_count
FROM actual150
HAVING COUNT(*) <> 2
    OR COUNT_IF(age IN (25,84) AND reporting_date = '2024-04-30'
        AND latest_record_date = '2023-04-30' AND is_in_numerator) <> 2
