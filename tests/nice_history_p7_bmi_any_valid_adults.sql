{{ config(tags=['monthly-full', 'nice-history']) }}

{% set ns = namespace(calculation=nice_ind320('by_month')) %}
{% for model, fixture in [
    ('int_nice_reference_population_by_month', 'synthetic_population'),
    ('int_nice_ltc_population_by_month', 'synthetic_ltc'),
    ('int_lipid_lowering_medications_all', 'synthetic_orders'),
    ('int_bmi_qof_all', 'synthetic_recorded'),
    ('int_bmi_all', 'synthetic_calculated'),
    ('int_cholesterol_ldl_all', 'synthetic_lipid'),
    ('int_cholesterol_hdl_all', 'synthetic_lipid'),
    ('int_triglycerides_all', 'synthetic_lipid')
] %}
    {% set ns.calculation = ns.calculation | replace(ref(model) | string, fixture) %}
{% endfor %}

WITH synthetic_population AS (
    SELECT
        people.column1::NUMBER AS person_id,
        dates.column1::DATE AS reporting_date,
        IFF(people.column1 = -7725, 17, 40) AS age,
        '1986-01-15'::DATE AS birth_date_approx,
        'Female' AS gender,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM VALUES (-7721), (-7722), (-7723), (-7724), (-7725), (-7726), (-7727) AS people
    CROSS JOIN (VALUES ('2026-09-30'), ('2026-10-31')) AS dates
),
synthetic_ltc AS (
    SELECT
        person_id,
        reporting_date,
        TRUE AS has_chd,
        FALSE AS has_stroke_tia,
        FALSE AS has_diabetes,
        FALSE AS has_ndh,
        FALSE AS has_hypertension,
        FALSE AS has_pad,
        FALSE AS has_heart_failure,
        FALSE AS has_copd,
        FALSE AS has_learning_disability,
        FALSE AS has_obstructive_sleep_apnoea,
        FALSE AS has_smi
    FROM synthetic_population
),
synthetic_orders AS (
    SELECT NULL::NUMBER AS person_id, NULL::DATE AS order_date,
        'STATIN' AS lipid_lowering_class
    WHERE FALSE
),
synthetic_lipid AS (
    SELECT NULL::NUMBER AS person_id, NULL::VARCHAR AS id,
        NULL::TIMESTAMP_NTZ AS clinical_effective_date,
        NULL::FLOAT AS cholesterol_value, NULL::FLOAT AS triglycerides_value,
        FALSE AS is_valid_cholesterol, FALSE AS is_valid_triglycerides
    WHERE FALSE
),
synthetic_recorded AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS clinical_effective_date,
        column3::FLOAT AS bmi_value,
        column4::VARCHAR AS source_cluster_id
    FROM VALUES
        (-7721, '2025-09-30', 10, 'BMIVAL_COD'),
        (-7721, '2025-09-30', 200, 'BMIVAL_COD'),
        (-7721, '2026-09-01', 200, 'BMIVAL_COD'),
        (-7721, '2026-10-01', 150, 'BMIVAL_COD'),
        (-7722, '2026-09-01', 200, 'BMIVAL_COD'),
        (-7723, '2026-09-01', 30, 'BMI30_COD'),
        (-7725, '2026-09-01', 25, 'BMIVAL_COD'),
        -- These records expire without replacement while CHD eligibility continues.
        (-7726, '2025-09-30', 25, 'BMIVAL_COD'),
        (-7727, '2025-09-30', 200, 'BMIVAL_COD')
),
synthetic_calculated AS (
    SELECT -7724::NUMBER AS person_id, '2026-09-30'::DATE AS clinical_effective_date,
        TRUE AS is_valid_bmi, 'calculated' AS bmi_source
),
actual AS ({{ ns.calculation }})
SELECT COUNT(*) AS failure_count
FROM actual
HAVING COUNT(*) <> 12
    OR COUNT_IF(person_id = -7721 AND reporting_date = '2026-09-30'
        AND is_in_numerator AND latest_record_date = '2025-09-30') <> 1
    OR COUNT_IF(person_id = -7721 AND reporting_date = '2026-10-31'
        AND is_in_numerator AND latest_record_date = '2026-10-01') <> 1
    OR COUNT_IF(person_id = -7722 AND NOT is_in_numerator
        AND indicator_status = 'NOT_ASSESSABLE') <> 2
    OR COUNT_IF(person_id = -7723 AND NOT is_in_numerator
        AND indicator_status = 'NOT_RECORDED_IN_PERIOD') <> 2
    OR COUNT_IF(person_id = -7724 AND is_in_numerator
        AND latest_bmi_date = '2026-09-30') <> 2
    OR COUNT_IF(person_id = -7726 AND reporting_date = '2026-09-30'
        AND is_in_numerator AND indicator_status = 'ACHIEVED'
        AND latest_record_date = '2025-09-30'
        AND latest_bmi_date = '2025-09-30') <> 1
    OR COUNT_IF(person_id = -7726 AND reporting_date = '2026-10-31'
        AND NOT is_in_numerator AND indicator_status = 'NOT_RECORDED_IN_PERIOD'
        AND latest_record_date IS NULL AND latest_bmi_date = '2025-09-30') <> 1
    OR COUNT_IF(person_id = -7727 AND reporting_date = '2026-09-30'
        AND NOT is_in_numerator AND indicator_status = 'NOT_ASSESSABLE'
        AND latest_record_date IS NULL AND latest_bmi_date IS NULL) <> 1
    OR COUNT_IF(person_id = -7727 AND reporting_date = '2026-10-31'
        AND NOT is_in_numerator AND indicator_status = 'NOT_RECORDED_IN_PERIOD'
        AND latest_record_date IS NULL AND latest_bmi_date IS NULL) <> 1
