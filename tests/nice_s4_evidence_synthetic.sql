{{ config(tags=['monthly-full', 'nice-history']) }}

{# Temporal fixtures call the actual S4 calculations, with synthetic evidence only. #}
{% set physical_health_sql = calculate_nice_physical_health_evidence('by_month') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set physical_health_sql = physical_health_sql | replace(ref('fct_person_diabetes_register_by_month') | string, 'synthetic_register') %}
{% set physical_health_sql = physical_health_sql | replace(ref('fct_person_smi_register_by_month') | string, 'synthetic_register') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_lithium_medications_all') | string, 'synthetic_orders') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_lithium_level_all') | string, 'synthetic_lithium') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_bmi_all') | string, 'synthetic_calculated_bmi') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_bmi_qof_all') | string, 'synthetic_recorded_bmi') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_blood_pressure_all') | string, 'synthetic_empty_events') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_cholesterol_all') | string, 'synthetic_empty_events') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_cholesterol_hdl_all') | string, 'synthetic_empty_events') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_cholesterol_ldl_all') | string, 'synthetic_empty_events') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_cholesterol_non_hdl_all') | string, 'synthetic_empty_events') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_cholesterol_hdl_ratio_all') | string, 'synthetic_empty_events') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_triglycerides_all') | string, 'synthetic_empty_events') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_non_numeric_lipid_test_all') | string, 'synthetic_non_numeric_lipids') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_hba1c_all') | string, 'synthetic_value_free_tests') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_blood_glucose_all') | string, 'synthetic_empty_events') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_alcohol_units_all') | string, 'synthetic_empty_events') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_alcohol_usage_all') | string, 'synthetic_empty_events') %}
{% set physical_health_sql = physical_health_sql | replace(ref('int_alcohol_screening_all') | string, 'synthetic_screens') %}
{% set alcohol_sql = calculate_nice_alcohol_evidence('by_month') %}
{% set alcohol_sql = alcohol_sql | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set alcohol_sql = alcohol_sql | replace(ref('fct_person_diabetes_register_by_month') | string, 'synthetic_register') %}
{% set alcohol_sql = alcohol_sql | replace(ref('fct_person_smi_register_by_month') | string, 'synthetic_register') %}
{% set alcohol_sql = alcohol_sql | replace(ref('fct_person_hypertension_register_by_month') | string, 'synthetic_register') %}
{% set alcohol_sql = alcohol_sql | replace(ref('fct_person_depression_register_by_month') | string, 'synthetic_register') %}
{% set alcohol_sql = alcohol_sql | replace(ref('fct_person_anxiety_register_by_month') | string, 'synthetic_register') %}
{% set alcohol_sql = alcohol_sql | replace(ref('fct_person_chd_register_by_month') | string, 'synthetic_register') %}
{% set alcohol_sql = alcohol_sql | replace(ref('fct_person_atrial_fibrillation_register_by_month') | string, 'synthetic_register') %}
{% set alcohol_sql = alcohol_sql | replace(ref('fct_person_heart_failure_register_by_month') | string, 'synthetic_register') %}
{% set alcohol_sql = alcohol_sql | replace(ref('fct_person_stroke_tia_register_by_month') | string, 'synthetic_register') %}
{% set alcohol_sql = alcohol_sql | replace(ref('fct_person_dementia_register_by_month') | string, 'synthetic_register') %}
{% set alcohol_sql = alcohol_sql | replace(ref('int_depression_diagnoses_all') | string, 'synthetic_depression') %}
{% set alcohol_sql = alcohol_sql | replace(ref('int_alcohol_screening_all') | string, 'synthetic_screens') %}
{% set alcohol_sql = alcohol_sql | replace(ref('int_alcohol_intervention') | string, 'synthetic_interventions') %}
{% set smoking_sql = calculate_nice_smoking_evidence('by_month') %}
{% set smoking_sql = smoking_sql | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set smoking_sql = smoking_sql | replace(ref('fct_person_ltc_summary_by_month') | string, 'synthetic_register') %}
{% set smoking_sql = smoking_sql | replace(ref('int_smoking_status_all') | string, 'synthetic_smoking') %}
{% set smoking_sql = smoking_sql | replace(ref('int_nice_smoking_support_all') | string, 'synthetic_support') %}

{% set physical_health_sql = physical_health_sql | replace('synthetic_register', 'synthetic_physical_register') %}
{% set previous_day_inputs = [] %}
{% for subject in ['physical_health', 'alcohol', 'smoking'] %}
{% set adapted = nice_ref('int_nice_' ~ subject ~ '_evidence', 'current') | string %}
{% set adapted = adapted | replace(ref('int_nice_' ~ subject ~ '_evidence') | string, 'synthetic_previous_day') %}
{% do previous_day_inputs.append('SELECT person_id, reporting_date FROM ' ~ adapted ~ ' AS previous_day') %}
{% endfor %}

WITH synthetic_population AS (
    SELECT
        person.column1::NUMBER AS person_id,
        date.column1::DATE AS reporting_date,
        person.column2::NUMBER AS age,
        '1980-01-01'::DATE AS birth_date_approx,
        'Female' AS gender,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM (VALUES (-9104, 45), (-9105, 14), (-9106, 20)) AS person
    CROSS JOIN (VALUES ('2026-03-31'), ('2026-04-30'), ('2026-09-30')) AS date

    UNION ALL

    -- R is after both interventions around the three-calendar-month boundary.
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        45::NUMBER AS age,
        '1980-01-01'::DATE AS birth_date_approx,
        'Female' AS gender,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM VALUES (-9107, '2026-06-30'), (-9107, '2026-07-31'), (-9108, '2026-06-30')
),

synthetic_register AS (
    SELECT
        person_id,
        reporting_date AS month_end_date,
        'DM' AS condition_code,
        '2020-01-01'::TIMESTAMP_NTZ AS earliest_diagnosis_date,
        '2020-01-01'::TIMESTAMP_NTZ AS latest_diagnosis_date,
        NULL::TIMESTAMP_NTZ AS latest_remission_date,
        NULL::TIMESTAMP_NTZ AS latest_resolved_date,
        'Type 2' AS diabetes_type,
        NULL::TIMESTAMP_NTZ AS earliest_type1_date,
        NULL::TIMESTAMP_NTZ AS latest_type1_date,
        '2020-01-01'::TIMESTAMP_NTZ AS earliest_type2_date,
        '2020-01-01'::TIMESTAMP_NTZ AS latest_type2_date
    FROM synthetic_population
    WHERE person_id IN (-9104, -9107, -9108)
),

synthetic_physical_register AS (
    SELECT *
    FROM synthetic_register
    UNION ALL
    SELECT
        child.person_id,
        child.reporting_date AS month_end_date,
        register.* EXCLUDE (person_id, month_end_date)
    FROM synthetic_population AS child
    INNER JOIN synthetic_register AS register
        ON child.reporting_date = register.month_end_date
    WHERE child.person_id = -9105
),

synthetic_previous_day AS (
    SELECT
        -9104::NUMBER AS person_id,
        DATEADD(day, -1, CURRENT_DATE())::DATE AS reporting_date
),

previous_day AS ({{ previous_day_inputs | join(' UNION ALL ') }}),

synthetic_depression AS (
    SELECT
        -9105::NUMBER AS person_id,
        '2026-03-01'::TIMESTAMP_NTZ AS clinical_effective_date,
        '2026-04-01'::TIMESTAMP_NTZ AS date_recorded,
        TRUE AS is_diagnosis_code,
        TRUE AS is_first_or_new_episode
),

synthetic_orders AS (
    SELECT
        -9106::NUMBER AS person_id,
        '2026-03-30'::DATE AS order_date,
        '2026-04-01'::DATE AS date_recorded
),

synthetic_empty_events AS (
    SELECT
        -9104::NUMBER AS person_id,
        'SYN_EMPTY' AS id,
        '2026-03-10'::TIMESTAMP_NTZ AS clinical_effective_date,
        '2026-03-10'::DATE AS effective_date,
        NULL::FLOAT AS cholesterol_value,
        NULL::FLOAT AS cholesterol_hdl_ratio,
        NULL::FLOAT AS triglycerides_value
    WHERE FALSE
),

synthetic_recorded_bmi AS (
    SELECT
        -9104::NUMBER AS person_id,
        column1::TIMESTAMP_NTZ AS clinical_effective_date,
        column2::VARCHAR AS source_cluster_id,
        column3::FLOAT AS bmi_value
    FROM VALUES ('2026-03-10', 'BMIVAL_COD', 25),
        ('2026-04-10', 'BMI30_COD', 30),
        ('2026-04-15', 'BMIVAL_COD', 5)
),

synthetic_calculated_bmi AS (
    SELECT
        column1::NUMBER AS person_id,
        '2026-04-18'::TIMESTAMP_NTZ AS clinical_effective_date,
        'calculated' AS bmi_source,
        TRUE AS is_valid_bmi
    FROM VALUES (-9104), (-9105)
),

synthetic_non_numeric_lipids AS (
    SELECT
        -9104::NUMBER AS person_id,
        column1::TIMESTAMP_NTZ AS clinical_effective_date
    FROM VALUES ('2026-03-15'), ('2026-04-15')
),

synthetic_value_free_tests AS (
    SELECT
        -9104::NUMBER AS person_id,
        '2026-03-20'::TIMESTAMP_NTZ AS clinical_effective_date
),

synthetic_lithium AS (
    SELECT
        -9104::NUMBER AS person_id,
        column1::VARCHAR AS id,
        column2::TIMESTAMP_NTZ AS clinical_effective_date,
        column3::FLOAT AS lithium_level,
        column4::BOOLEAN AS is_result_recorded,
        column4::BOOLEAN AS is_in_therapeutic_range
    FROM VALUES ('SYN_A', '2026-03-20', NULL, FALSE),
        ('SYN_B', '2026-03-20', 0.5, TRUE),
        ('SYN_C', '2026-04-20', NULL, FALSE)
),

synthetic_screens AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::VARCHAR AS id,
        column3::TIMESTAMP_NTZ AS clinical_effective_date,
        'FAST_COD' AS source_cluster_id,
        'FAST' AS screening_tool,
        column4::FLOAT AS score_value,
        column4 >= 3 AS is_positive_screen
    FROM VALUES (-9104, 'SYN_S1', '2026-03-01', 3),
        (-9104, 'SYN_S2', '2026-04-05', 0),
        (-9104, 'SYN_S3', '2026-08-01', 3),
        (-9107, 'SYN_BOUNDARY1', '2026-03-01', 3),
        (-9108, 'SYN_BOUNDARY2', '2026-03-01', 3)
),

synthetic_interventions AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::TIMESTAMP_NTZ AS clinical_effective_date,
        column3::VARCHAR AS alcohol_advice_services
    FROM VALUES (-9104, '2026-03-31', 'Yes'),
        (-9104, '2026-04-15', 'Yes'),
        (-9104, '2026-04-30', 'Declined'),
        (-9104, '2026-05-01', 'Yes'),
        (-9104, '2026-08-15', 'Declined'),
        (-9107, '2026-06-01', 'Yes'),
        (-9107, '2026-06-02', 'Yes'),
        (-9108, '2026-06-02', 'Yes')
),

synthetic_smoking AS (
    SELECT
        -9104::NUMBER AS person_id,
        column1::VARCHAR AS id,
        column2::TIMESTAMP_NTZ AS clinical_effective_date,
        column3::VARCHAR AS source_cluster_id,
        column4::VARCHAR AS smoking_status,
        column3 = 'NSMOK_COD' AS is_never_smoked_code
    FROM VALUES ('SYN_Z', '2026-03-01', 'NSMOK_COD', 'Never Smoked'),
        ('SYN_A', '2026-03-01', 'LSMOK_COD', 'Current Smoker'),
        ('SYN_B', '2026-04-01', 'EXSMOK_COD', 'Ex-Smoker'),
        ('SYN_C', '2026-08-01', 'SMOK_COD', 'Non-Smoker (History Unknown)')
),

synthetic_support AS (
    SELECT
        -9104::NUMBER AS person_id,
        column1::DATE AS event_date
    FROM VALUES ('2026-03-31'), ('2026-04-30'), ('2026-10-01')
),

physical AS ({{ physical_health_sql }}),
alcohol AS ({{ alcohol_sql }}),
smoking AS ({{ smoking_sql }})

SELECT 'physical' AS failed_rule
FROM physical
HAVING COUNT(*) <> 10
    OR COUNT_IF(person_id = -9106 AND reporting_date = '2026-04-30') <> 1
    OR COUNT_IF(person_id = -9105 AND latest_bmi_date IS NOT NULL) <> 0
    OR COUNT_IF(person_id = -9104 AND reporting_date = '2026-03-31'
        AND latest_bmi_date = '2026-03-10'
        AND latest_lipid_date = '2026-03-15'
        AND latest_glucose_or_hba1c_date = '2026-03-20'
        AND latest_lithium_level = 0.5
        AND is_latest_lithium_level_in_range) <> 1
    OR COUNT_IF(person_id = -9104 AND reporting_date = '2026-04-30'
        AND latest_bmi_date = '2026-04-18'
        AND latest_lipid_date = '2026-04-15'
        AND latest_lithium_level_date = '2026-04-20'
        AND latest_lithium_level IS NULL
        AND NOT is_latest_lithium_level_in_range) <> 1

UNION ALL

SELECT 'alcohol' AS failed_rule
FROM alcohol
HAVING COUNT(*) <> 8
    OR COUNT_IF(person_id = -9105 AND reporting_date = '2026-03-31') <> 0
    OR COUNT_IF(person_id = -9104 AND reporting_date = '2026-03-31'
        AND latest_alcohol_screen_date = '2026-03-01'
        AND latest_intervention_after_positive_screen_date = '2026-03-31') <> 1
    OR COUNT_IF(person_id = -9104 AND reporting_date = '2026-04-30'
        AND latest_alcohol_screen_date = '2026-04-05'
        AND NOT is_latest_alcohol_screen_positive
        AND latest_positive_alcohol_screen_date = '2026-03-01'
        AND latest_intervention_after_positive_screen_date = '2026-04-15') <> 1
    OR COUNT_IF(person_id = -9104 AND reporting_date = '2026-09-30'
        AND latest_positive_alcohol_screen_date = '2026-08-01'
        AND latest_intervention_after_positive_screen_date IS NULL) <> 1
    OR COUNT_IF(person_id = -9107
        AND reporting_date IN ('2026-06-30', '2026-07-31')
        AND latest_positive_alcohol_screen_date = '2026-03-01'
        AND latest_intervention_after_positive_screen_date = '2026-06-01') <> 2
    OR COUNT_IF(person_id = -9108 AND reporting_date = '2026-06-30'
        AND latest_positive_alcohol_screen_date = '2026-03-01'
        AND latest_intervention_after_positive_screen_date IS NULL) <> 1

UNION ALL

SELECT 'smoking' AS failed_rule
FROM smoking
HAVING COUNT(*) <> 6
    OR COUNT_IF(reporting_date = '2026-03-31' AND latest_smoking_status = 'Current Smoker'
        AND latest_never_smoked_date = '2026-03-01'
        AND latest_smoking_intervention_date = '2026-03-31') <> 1
    OR COUNT_IF(reporting_date = '2026-04-30' AND latest_smoking_status = 'Ex-Smoker'
        AND latest_smoking_intervention_date = '2026-04-30') <> 1
    OR COUNT_IF(reporting_date = '2026-09-30' AND latest_smoking_status = 'Non-Smoker (History Unknown)'
        AND latest_never_smoked_date = '2026-03-01'
        AND latest_smoking_intervention_date = '2026-04-30') <> 1

UNION ALL

SELECT 'previous_day' AS failed_rule
FROM previous_day
HAVING COUNT(*) <> 3
    OR COUNT_IF(reporting_date = CURRENT_DATE()) <> 3
