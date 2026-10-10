{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation82 = nice_ind82('current') %}
{% set calculation82 = calculation82 | replace(nice_reference_population('current'), 'SELECT * FROM synthetic_population') %}
{% set calculation82 = calculation82 | replace(ref('int_nice_ltc_population') | string, 'synthetic_ltc') %}
{% set calculation82 = calculation82 | replace(ref('int_nice_physical_health_evidence') | string, 'synthetic_physical') %}
{% set calculation82 = calculation82 | replace(ref('int_nice_cervical_screening_evidence') | string, 'synthetic_cervical') %}
{% set calculation82 = calculation82 | replace(ref('int_nice_review_evidence') | string, 'synthetic_review') %}
{% set calculation82 = calculation82 | replace(ref('int_cvd_risk_profile') | string, 'synthetic_cvd') %}
{% set calculation83 = nice_ind83('current') %}
{% set calculation83 = calculation83 | replace(nice_reference_population('current'), 'SELECT * FROM synthetic_population') %}
{% set calculation83 = calculation83 | replace(ref('int_nice_ltc_population') | string, 'synthetic_ltc') %}
{% set calculation83 = calculation83 | replace(ref('int_nice_physical_health_evidence') | string, 'synthetic_physical') %}
{% set calculation83 = calculation83 | replace(ref('int_nice_cervical_screening_evidence') | string, 'synthetic_cervical') %}
{% set calculation83 = calculation83 | replace(ref('int_nice_review_evidence') | string, 'synthetic_review') %}
{% set calculation83 = calculation83 | replace(ref('int_cvd_risk_profile') | string, 'synthetic_cvd') %}
{% set calculation84 = nice_ind84('current') %}
{% set calculation84 = calculation84 | replace(nice_reference_population('current'), 'SELECT * FROM synthetic_population') %}
{% set calculation84 = calculation84 | replace(ref('int_nice_ltc_population') | string, 'synthetic_ltc') %}
{% set calculation84 = calculation84 | replace(ref('int_nice_physical_health_evidence') | string, 'synthetic_physical') %}
{% set calculation84 = calculation84 | replace(ref('int_nice_cervical_screening_evidence') | string, 'synthetic_cervical') %}
{% set calculation84 = calculation84 | replace(ref('int_nice_review_evidence') | string, 'synthetic_review') %}
{% set calculation84 = calculation84 | replace(ref('int_cvd_risk_profile') | string, 'synthetic_cvd') %}
{% set calculation85 = nice_ind85('current') %}
{% set calculation85 = calculation85 | replace(nice_reference_population('current'), 'SELECT * FROM synthetic_population') %}
{% set calculation85 = calculation85 | replace(ref('int_nice_ltc_population') | string, 'synthetic_ltc') %}
{% set calculation85 = calculation85 | replace(ref('int_nice_physical_health_evidence') | string, 'synthetic_physical') %}
{% set calculation85 = calculation85 | replace(ref('int_nice_cervical_screening_evidence') | string, 'synthetic_cervical') %}
{% set calculation85 = calculation85 | replace(ref('int_nice_review_evidence') | string, 'synthetic_review') %}
{% set calculation85 = calculation85 | replace(ref('int_cvd_risk_profile') | string, 'synthetic_cvd') %}
{% set calculation87 = nice_ind87('current') %}
{% set calculation87 = calculation87 | replace(nice_reference_population('current'), 'SELECT * FROM synthetic_population') %}
{% set calculation87 = calculation87 | replace(ref('int_nice_ltc_population') | string, 'synthetic_ltc') %}
{% set calculation87 = calculation87 | replace(ref('int_nice_physical_health_evidence') | string, 'synthetic_physical') %}
{% set calculation87 = calculation87 | replace(ref('int_nice_cervical_screening_evidence') | string, 'synthetic_cervical') %}
{% set calculation87 = calculation87 | replace(ref('int_nice_review_evidence') | string, 'synthetic_review') %}
{% set calculation87 = calculation87 | replace(ref('int_cvd_risk_profile') | string, 'synthetic_cvd') %}
{% set calculation143 = nice_ind143('current') %}
{% set calculation143 = calculation143 | replace(nice_reference_population('current'), 'SELECT * FROM synthetic_population') %}
{% set calculation143 = calculation143 | replace(ref('int_nice_ltc_population') | string, 'synthetic_ltc') %}
{% set calculation143 = calculation143 | replace(ref('int_nice_physical_health_evidence') | string, 'synthetic_physical') %}
{% set calculation143 = calculation143 | replace(ref('int_nice_cervical_screening_evidence') | string, 'synthetic_cervical') %}
{% set calculation143 = calculation143 | replace(ref('int_nice_review_evidence') | string, 'synthetic_review') %}
{% set calculation143 = calculation143 | replace(ref('int_cvd_risk_profile') | string, 'synthetic_cvd') %}
{% set calculation150 = nice_ind150('current') %}
{% set calculation150 = calculation150 | replace(nice_reference_population('current'), 'SELECT * FROM synthetic_population') %}
{% set calculation150 = calculation150 | replace(ref('int_nice_ltc_population') | string, 'synthetic_ltc') %}
{% set calculation150 = calculation150 | replace(ref('int_nice_physical_health_evidence') | string, 'synthetic_physical') %}
{% set calculation150 = calculation150 | replace(ref('int_nice_cervical_screening_evidence') | string, 'synthetic_cervical') %}
{% set calculation150 = calculation150 | replace(ref('int_nice_review_evidence') | string, 'synthetic_review') %}
{% set calculation150 = calculation150 | replace(ref('int_cvd_risk_profile') | string, 'synthetic_cvd') %}
WITH synthetic_population AS (
    SELECT -10131::NUMBER AS person_id, CURRENT_DATE()::DATE AS reporting_date,
        40 AS age, 'Female' AS gender, 'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name, '1984-01-01'::DATE AS birth_date_approx
),
synthetic_ltc AS (
    SELECT person_id, DATEADD(day,-1,reporting_date)::DATE AS reporting_date,
        TRUE AS has_active_smi_diagnosis, TRUE AS is_on_lithium,
        DATEADD(year,-3,CURRENT_DATE())::DATE AS earliest_smi_diagnosis_date,
        DATEADD(year,-1,CURRENT_DATE())::DATE AS latest_smi_diagnosis_date,
        NULL::DATE AS latest_smi_remission_date, NULL::DATE AS earliest_cvd_diagnosis_date
    FROM synthetic_population
),
synthetic_physical AS (
    SELECT person_id, reporting_date, reporting_date AS latest_bmi_date,
        reporting_date AS latest_blood_pressure_date,
        reporting_date AS latest_alcohol_record_date,
        reporting_date AS latest_lithium_level_date, 0.4::FLOAT AS latest_lithium_level,
        TRUE AS is_latest_lithium_level_in_range
    FROM synthetic_ltc
),
synthetic_cervical AS (
    SELECT person_id, reporting_date, reporting_date AS latest_completed_date FROM synthetic_ltc
),
synthetic_review AS (
    SELECT person_id, reporting_date, reporting_date AS latest_smi_care_plan_date FROM synthetic_ltc
),
synthetic_cvd AS (
    SELECT person_id, reporting_date, FALSE AS has_ckd,
        FALSE AS has_familial_hypercholesterolaemia, FALSE AS has_type1_diabetes,
        reporting_date AS latest_risk_assessment_date
    FROM synthetic_ltc
),
actual82 AS ({{ calculation82 }}),
actual83 AS ({{ calculation83 }}),
actual84 AS ({{ calculation84 }}),
actual85 AS ({{ calculation85 }}),
actual87 AS ({{ calculation87 }}),
actual143 AS ({{ calculation143 }}),
actual150 AS ({{ calculation150 }})
SELECT COUNT(*) AS failure_count
FROM (
    SELECT indicator_id, reporting_date, is_in_numerator FROM actual82
    UNION ALL
    SELECT indicator_id, reporting_date, is_in_numerator FROM actual83
    UNION ALL
    SELECT indicator_id, reporting_date, is_in_numerator FROM actual84
    UNION ALL
    SELECT indicator_id, reporting_date, is_in_numerator FROM actual85
    UNION ALL
    SELECT indicator_id, reporting_date, is_in_numerator FROM actual87
    UNION ALL
    SELECT indicator_id, reporting_date, is_in_numerator FROM actual143
    UNION ALL
    SELECT indicator_id, reporting_date, is_in_numerator FROM actual150
)
HAVING COUNT(*) <> 7
    OR COUNT_IF(reporting_date = CURRENT_DATE() AND is_in_numerator) <> 7
