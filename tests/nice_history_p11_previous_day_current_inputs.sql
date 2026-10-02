{{ config(tags=['monthly-full', 'nice-history']) }}

{% set actual_154 = nice_ind154('current') %}
{% set actual_154 = actual_154 | replace(nice_reference_population('current'), 'SELECT * FROM synthetic_population') %}
{% set actual_154 = actual_154 | replace(ref('int_nice_ltc_population') | string, 'synthetic_ltc') %}
{% set actual_154 = actual_154 | replace(ref('int_nice_smoking_evidence') | string, 'synthetic_smoking') %}
{% set actual_154 = actual_154 | replace(ref('int_nice_physical_health_evidence') | string, 'synthetic_physical') %}
{% set actual_154 = actual_154 | replace(ref('int_nice_cervical_screening_evidence') | string, 'synthetic_screening') %}
{% set actual_155 = nice_ind155('current') %}
{% set actual_155 = actual_155 | replace(nice_reference_population('current'), 'SELECT * FROM synthetic_population') %}
{% set actual_155 = actual_155 | replace(ref('int_nice_ltc_population') | string, 'synthetic_ltc') %}
{% set actual_155 = actual_155 | replace(ref('int_nice_smoking_evidence') | string, 'synthetic_smoking') %}
{% set actual_155 = actual_155 | replace(ref('int_nice_physical_health_evidence') | string, 'synthetic_physical') %}
{% set actual_155 = actual_155 | replace(ref('int_nice_cervical_screening_evidence') | string, 'synthetic_screening') %}
{% set actual_158 = nice_ind158('current') %}
{% set actual_158 = actual_158 | replace(nice_reference_population('current'), 'SELECT * FROM synthetic_population') %}
{% set actual_158 = actual_158 | replace(ref('int_nice_ltc_population') | string, 'synthetic_ltc') %}
{% set actual_158 = actual_158 | replace(ref('int_nice_smoking_evidence') | string, 'synthetic_smoking') %}
{% set actual_158 = actual_158 | replace(ref('int_nice_physical_health_evidence') | string, 'synthetic_physical') %}
{% set actual_158 = actual_158 | replace(ref('int_nice_cervical_screening_evidence') | string, 'synthetic_screening') %}
{% set actual_159 = nice_ind159('current') %}
{% set actual_159 = actual_159 | replace(nice_reference_population('current'), 'SELECT * FROM synthetic_population') %}
{% set actual_159 = actual_159 | replace(ref('int_nice_ltc_population') | string, 'synthetic_ltc') %}
{% set actual_159 = actual_159 | replace(ref('int_nice_smoking_evidence') | string, 'synthetic_smoking') %}
{% set actual_159 = actual_159 | replace(ref('int_nice_physical_health_evidence') | string, 'synthetic_physical') %}
{% set actual_159 = actual_159 | replace(ref('int_nice_cervical_screening_evidence') | string, 'synthetic_screening') %}
{% set actual_213 = nice_ind213('current') %}
{% set actual_213 = actual_213 | replace(nice_reference_population('current'), 'SELECT * FROM synthetic_population') %}
{% set actual_213 = actual_213 | replace(ref('int_nice_ltc_population') | string, 'synthetic_ltc') %}
{% set actual_213 = actual_213 | replace(ref('int_nice_smoking_evidence') | string, 'synthetic_smoking') %}
{% set actual_213 = actual_213 | replace(ref('int_nice_physical_health_evidence') | string, 'synthetic_physical') %}
{% set actual_213 = actual_213 | replace(ref('int_nice_cervical_screening_evidence') | string, 'synthetic_screening') %}
{% set actual_214 = nice_ind214('current') %}
{% set actual_214 = actual_214 | replace(nice_reference_population('current'), 'SELECT * FROM synthetic_population') %}
{% set actual_214 = actual_214 | replace(ref('int_nice_ltc_population') | string, 'synthetic_ltc') %}
{% set actual_214 = actual_214 | replace(ref('int_nice_smoking_evidence') | string, 'synthetic_smoking') %}
{% set actual_214 = actual_214 | replace(ref('int_nice_physical_health_evidence') | string, 'synthetic_physical') %}
{% set actual_214 = actual_214 | replace(ref('int_nice_cervical_screening_evidence') | string, 'synthetic_screening') %}
{% set actual_248 = nice_ind248('current') %}
{% set actual_248 = actual_248 | replace(nice_reference_population('current'), 'SELECT * FROM synthetic_population') %}
{% set actual_248 = actual_248 | replace(ref('int_nice_ltc_population') | string, 'synthetic_ltc') %}
{% set actual_248 = actual_248 | replace(ref('int_nice_smoking_evidence') | string, 'synthetic_smoking') %}
{% set actual_248 = actual_248 | replace(ref('int_nice_physical_health_evidence') | string, 'synthetic_physical') %}
{% set actual_248 = actual_248 | replace(ref('int_nice_cervical_screening_evidence') | string, 'synthetic_screening') %}

WITH synthetic_population AS (
    SELECT -11051::NUMBER AS person_id, CURRENT_DATE()::DATE AS reporting_date,
        40::NUMBER AS age, '1986-01-01'::DATE AS birth_date_approx,
        'Female'::VARCHAR AS gender, 'SYNTHETIC'::VARCHAR AS practice_code,
        'Synthetic practice'::VARCHAR AS practice_name
),
synthetic_ltc AS (
    SELECT -11051::NUMBER AS person_id, DATEADD(day, -1, CURRENT_DATE())::DATE AS reporting_date,
        TRUE AS has_active_smi_diagnosis, '2020-01-01'::DATE AS earliest_smi_diagnosis_date,
        NULL::DATE AS earliest_cvd_diagnosis_date, NULL::DATE AS earliest_diabetes_diagnosis_date
),
synthetic_smoking AS (
    SELECT -11051::NUMBER AS person_id, DATEADD(day, -1, CURRENT_DATE())::DATE AS reporting_date,
        'Current Smoker'::VARCHAR AS latest_smoking_status,
        DATEADD(day, -1, CURRENT_DATE())::DATE AS latest_smoking_status_date,
        NULL::DATE AS latest_never_smoked_date,
        DATEADD(day, -1, CURRENT_DATE())::DATE AS latest_smoking_intervention_date
),
synthetic_physical AS (
    SELECT -11051::NUMBER AS person_id, DATEADD(day, -1, CURRENT_DATE())::DATE AS reporting_date,
        reporting_date AS latest_blood_pressure_date, reporting_date AS latest_bmi_date,
        reporting_date AS latest_alcohol_record_date, reporting_date AS latest_lipid_date,
        reporting_date AS latest_glucose_or_hba1c_date, reporting_date AS latest_cholesterol_hdl_ratio_date
),
synthetic_screening AS (
    SELECT -11051::NUMBER AS person_id, DATEADD(day, -1, CURRENT_DATE())::DATE AS reporting_date,
        reporting_date::TIMESTAMP_NTZ AS latest_completed_date
),
actual_154 AS ({{ actual_154 }}),
actual_155 AS ({{ actual_155 }}),
actual_158 AS ({{ actual_158 }}),
actual_159 AS ({{ actual_159 }}),
actual_213 AS ({{ actual_213 }}),
actual_214 AS ({{ actual_214 }}),
actual_248 AS ({{ actual_248 }})
, actual AS (
SELECT indicator_id, reporting_date, is_in_numerator FROM actual_154
UNION ALL
SELECT indicator_id, reporting_date, is_in_numerator FROM actual_155
UNION ALL
SELECT indicator_id, reporting_date, is_in_numerator FROM actual_158
UNION ALL
SELECT indicator_id, reporting_date, is_in_numerator FROM actual_159
UNION ALL
SELECT indicator_id, reporting_date, is_in_numerator FROM actual_213
UNION ALL
SELECT indicator_id, reporting_date, is_in_numerator FROM actual_214
UNION ALL
SELECT indicator_id, reporting_date, is_in_numerator FROM actual_248
)
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 6
    OR COUNT_IF(reporting_date = CURRENT_DATE() AND is_in_numerator) <> 6
