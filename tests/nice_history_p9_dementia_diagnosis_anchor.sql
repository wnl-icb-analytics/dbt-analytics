{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = nice_ind142('by_month') %}
{% set calculation = calculation | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set calculation = calculation | replace(ref('int_nice_ltc_population_by_month') | string, 'synthetic_profile') %}
{% set calculation = calculation | replace(ref('int_nice_review_evidence_by_month') | string, 'synthetic_reviews') %}

WITH synthetic_population AS (
    SELECT
        -9903::NUMBER AS person_id,
        column1::DATE AS reporting_date,
        70 AS age,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM VALUES ('2024-09-30'), ('2024-10-31')
),
synthetic_profile AS (
    SELECT person_id, reporting_date, TRUE AS has_copd, TRUE AS has_heart_failure,
        TRUE AS has_dementia, '2023-10-01'::DATE AS earliest_dementia_diagnosis_date
    FROM synthetic_population
),
synthetic_reviews AS (
    SELECT person_id, reporting_date,
        '2023-09-30'::DATE AS latest_copd_review_date,
        '2024-09-01'::DATE AS latest_mrc_dyspnoea_date,
        '2024-09-02'::DATE AS latest_copd_exacerbation_count_date,
        '2024-09-03'::DATE AS latest_heart_failure_review_date,
        '2023-09-30'::DATE AS latest_nyha_date,
        '2024-09-04'::DATE AS latest_medication_review_date,
        IFF(reporting_date = '2024-09-30', '2023-09-30', '2024-10-01')::DATE
            AS latest_dementia_care_plan_date
    FROM synthetic_population
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 2
    OR COUNT_IF(reporting_date = '2024-09-30' AND NOT is_in_numerator AND latest_record_date = '2023-09-30') <> 1
    OR COUNT_IF(reporting_date = '2024-10-31' AND is_in_numerator AND latest_record_date = '2024-10-01') <> 1
