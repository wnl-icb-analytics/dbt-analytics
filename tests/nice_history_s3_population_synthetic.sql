{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = calculate_nice_ltc_population('by_month') %}
{% set replacements = {
    'int_nice_reference_population_by_month': 'synthetic_population',
    'fct_person_ltc_summary_by_month': 'synthetic_summary',
    'int_depression_diagnoses_all': 'synthetic_depression',
    'fct_person_smi_register_by_month': 'synthetic_smi',
    'fct_person_frailty_register_by_month': 'synthetic_frailty',
    'int_lithium_medications_all': 'synthetic_lithium',
    'int_lithium_stop_all': 'synthetic_stops',
    'int_alcohol_misuse_disorders': 'synthetic_history',
    'int_nice_alcohol_misuse_all': 'synthetic_history',
    'int_dyslipidaemia_diagnoses_all': 'synthetic_history',
    'int_obstructive_sleep_apnoea_diagnoses_all': 'synthetic_history',
    'int_cvd_secondary_prevention_population_by_month': 'synthetic_cvd'
} %}
{% set query = namespace(sql=calculation | replace(nice_reference_dates('by_month') | string, 'SELECT reporting_date FROM synthetic_dates')) %}
{% for model, fixture in replacements.items() %}
    {% set query.sql = query.sql | replace(ref(model) | string, fixture) %}
{% endfor %}

WITH synthetic_dates AS (
    SELECT column1::DATE AS reporting_date
    FROM VALUES ('2026-08-31'), ('2026-09-30')
),
synthetic_population AS (
    SELECT
        people.column1::NUMBER AS person_id,
        dates.reporting_date,
        15 AS age,
        '2011-01-01'::DATE AS birth_date_approx,
        'Female' AS gender,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM (VALUES (-9301), (-9302), (-9303)) AS people
    CROSS JOIN synthetic_dates AS dates
),
synthetic_summary AS (
    SELECT
        person_id,
        reporting_date AS month_end_date,
        'AF' AS condition_code,
        '2020-01-01'::TIMESTAMP_NTZ AS earliest_diagnosis_date,
        '2020-01-01'::TIMESTAMP_NTZ AS latest_diagnosis_date
    FROM synthetic_population
    WHERE person_id IN (-9302, -9303)
),
synthetic_depression AS (
    SELECT
        -9301::NUMBER AS person_id,
        column1::TIMESTAMP_NTZ AS clinical_effective_date,
        column2::TIMESTAMP_NTZ AS date_recorded,
        column3::BOOLEAN AS is_diagnosis_code,
        column3::BOOLEAN AS is_first_or_new_episode,
        NOT column3::BOOLEAN AS is_resolved_code
    FROM VALUES
        ('2026-08-01', '2026-08-01', TRUE),
        ('2026-08-20', '2026-09-05', TRUE),
        ('2026-08-21', '2026-09-15', FALSE),
        ('2026-07-01', '2026-09-20', TRUE)
),
synthetic_smi AS (
    SELECT
        person_id,
        reporting_date AS month_end_date,
        NULL::TIMESTAMP_NTZ AS earliest_diagnosis_date,
        NULL::TIMESTAMP_NTZ AS latest_diagnosis_date,
        NULL::TIMESTAMP_NTZ AS latest_remission_date
    FROM synthetic_population
    WHERE FALSE
),
synthetic_frailty AS (
    SELECT
        person_id,
        reporting_date AS month_end_date,
        NULL::TIMESTAMP_NTZ AS earliest_diagnosis_date,
        NULL::TIMESTAMP_NTZ AS latest_diagnosis_date,
        NULL::VARCHAR AS latest_frailty_severity
    FROM synthetic_population
    WHERE FALSE
),
synthetic_lithium AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::TIMESTAMP_NTZ AS order_date,
        column3::TIMESTAMP_NTZ AS date_recorded
    FROM VALUES
        (-9302, '2026-03-30', '2026-03-30'),
        (-9302, '2026-09-10', '2026-10-10'),
        (-9303, '2026-08-01', '2026-08-01')
),
synthetic_stops AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::TIMESTAMP_NTZ AS clinical_effective_date,
        column3::TIMESTAMP_NTZ AS date_recorded
    FROM VALUES
        (-9302, '2026-03-30', '2026-03-30'),
        (-9303, '2026-08-02', '2026-09-15')
),
synthetic_history AS (
    SELECT
        -9303::NUMBER AS person_id,
        '2026-09-01'::TIMESTAMP_NTZ AS clinical_effective_date
),
synthetic_cvd AS (
    SELECT
        person_id,
        reporting_date,
        NULL::DATE AS earliest_cvd_diagnosis_date
    FROM synthetic_population
    WHERE FALSE
),
actual AS ({{ query.sql }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 6
    OR COUNT_IF(person_id = -9301 AND reporting_date = '2026-08-31'
        AND earliest_depression_anxiety_date = '2026-08-01'
        AND latest_new_depression_diagnosis_date = '2026-08-01') <> 1
    OR COUNT_IF(person_id = -9301 AND reporting_date = '2026-09-30'
        AND earliest_depression_anxiety_date IS NULL
        AND latest_new_depression_diagnosis_date = '2026-08-20') <> 1
    OR COUNT_IF(person_id = -9302 AND reporting_date = '2026-08-31'
        AND is_on_lithium AND NOT has_smi) <> 1
    OR COUNT_IF(person_id = -9302 AND reporting_date = '2026-09-30'
        AND NOT is_on_lithium) <> 1
    OR COUNT_IF(person_id = -9303 AND reporting_date = '2026-08-31'
        AND is_on_lithium AND NOT has_nice_alcohol_disorder) <> 1
    OR COUNT_IF(person_id = -9303 AND reporting_date = '2026-09-30'
        AND NOT is_on_lithium AND has_nice_alcohol_disorder) <> 1
