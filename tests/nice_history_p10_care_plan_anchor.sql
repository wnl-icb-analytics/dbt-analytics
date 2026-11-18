{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation143 = nice_ind143('by_month') %}
{% set calculation143 = calculation143 | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation143 = calculation143 | replace(ref('int_nice_ltc_population_by_month') | string, 'synthetic_ltc') %}
{% set calculation143 = calculation143 | replace(ref('int_nice_review_evidence_by_month') | string, 'synthetic_review') %}
WITH synthetic_population AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS reporting_date,
        40 AS age, 'Female' AS gender, 'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name, '1984-01-01'::DATE AS birth_date_approx
    FROM VALUES (-10111,'2024-04-30'), (-10111,'2024-05-31'),
        (-10112,'2024-04-30'), (-10113,'2024-04-30'), (-10114,'2024-04-30')
),
synthetic_ltc AS (
    SELECT person_id, reporting_date, person_id <> -10114 AS has_active_smi_diagnosis,
        '2020-01-01'::DATE AS earliest_smi_diagnosis_date,
        '2024-04-01'::DATE AS latest_smi_diagnosis_date,
        IFF(person_id IN (-10112,-10113), '2024-03-01'::DATE, NULL) AS latest_smi_remission_date
    FROM synthetic_population
),
synthetic_review AS (
    SELECT person_id, reporting_date,
        CASE person_id WHEN -10111 THEN '2023-04-30'::DATE
            WHEN -10112 THEN '2024-03-31'::DATE ELSE '2024-04-01'::DATE END AS latest_smi_care_plan_date
    FROM synthetic_population
),
actual143 AS ({{ calculation143 }})
SELECT COUNT(*) AS failure_count
FROM actual143
HAVING COUNT(*) <> 4
    OR COUNT_IF(person_id = -10111 AND reporting_date = '2024-04-30'
        AND plan_anchor_date = '2020-01-01' AND latest_record_date = '2023-04-30'
        AND is_in_numerator) <> 1
    OR COUNT_IF(person_id = -10111 AND reporting_date = '2024-05-31'
        AND plan_anchor_date = '2020-01-01' AND latest_record_date IS NULL
        AND NOT is_in_numerator) <> 1
    OR COUNT_IF(person_id = -10112 AND plan_anchor_date = '2024-04-01'
        AND latest_record_date = '2024-03-31' AND NOT is_in_numerator) <> 1
    OR COUNT_IF(person_id = -10113 AND plan_anchor_date = '2024-04-01'
        AND latest_record_date = '2024-04-01' AND is_in_numerator) <> 1
