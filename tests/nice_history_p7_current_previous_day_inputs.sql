{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculations = [] %}
{% for indicator in [97, 156, 157, 320] %}
    {% set ns = namespace(calculation=nice_ind97('current') if indicator == 97
        else nice_ind156('current') if indicator == 156
        else nice_ind157('current') if indicator == 157 else nice_ind320('current')) %}
    {% set ns.calculation = ns.calculation | replace(nice_reference_population('current'), 'SELECT * FROM synthetic_population') %}
    {% for model, fixture in [
        ('int_nice_ltc_population', 'synthetic_ltc'),
        ('int_nice_smoking_evidence', 'synthetic_smoking'),
        ('int_lipid_lowering_medications_all', 'synthetic_orders'),
        ('int_bmi_qof_all', 'synthetic_recorded'),
        ('int_bmi_all', 'synthetic_calculated'),
        ('int_cholesterol_ldl_all', 'synthetic_lipid'),
        ('int_cholesterol_hdl_all', 'synthetic_lipid'),
        ('int_triglycerides_all', 'synthetic_lipid')
    ] %}
        {% set ns.calculation = ns.calculation | replace(ref(model) | string, fixture) %}
    {% endfor %}
    {% do calculations.append(ns.calculation) %}
{% endfor %}

WITH synthetic_population AS (
    SELECT -7740::NUMBER AS person_id, CURRENT_DATE()::DATE AS reporting_date,
        40 AS age, '1986-01-15'::DATE AS birth_date_approx,
        'Female' AS gender, 'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
),
synthetic_ltc AS (
    SELECT person_id, DATEADD(day, -1, reporting_date)::DATE AS reporting_date,
        birth_date_approx, TRUE AS has_chd, FALSE AS has_pad,
        FALSE AS has_stroke_tia, FALSE AS has_hypertension,
        FALSE AS has_diabetes, FALSE AS has_copd, FALSE AS has_ckd,
        FALSE AS has_asthma, FALSE AS has_ndh, FALSE AS has_heart_failure,
        FALSE AS has_learning_disability, FALSE AS has_obstructive_sleep_apnoea,
        FALSE AS has_smi, NULL::DATE AS earliest_smi_diagnosis_date,
        '2020-01-01'::DATE AS earliest_smoking_ltc_diagnosis_date,
        '2020-01-01'::DATE AS earliest_smoking_smi_ltc_diagnosis_date
    FROM synthetic_population
),
synthetic_smoking AS (
    SELECT person_id, reporting_date, 'Current Smoker' AS latest_smoking_status,
        reporting_date AS latest_smoking_status_date,
        NULL::DATE AS latest_never_smoked_date,
        reporting_date AS latest_smoking_intervention_date
    FROM synthetic_ltc
),
synthetic_orders AS (
    SELECT NULL::NUMBER AS person_id, NULL::DATE AS order_date,
        'STATIN' AS lipid_lowering_class
    WHERE FALSE
),
synthetic_recorded AS (
    SELECT person_id, reporting_date AS clinical_effective_date,
        25::FLOAT AS bmi_value, 'BMIVAL_COD' AS source_cluster_id
    FROM synthetic_ltc
),
synthetic_calculated AS (
    SELECT NULL::NUMBER AS person_id, NULL::DATE AS clinical_effective_date,
        FALSE AS is_valid_bmi, 'calculated' AS bmi_source
    WHERE FALSE
),
synthetic_lipid AS (
    SELECT NULL::NUMBER AS person_id, NULL::VARCHAR AS id,
        NULL::TIMESTAMP_NTZ AS clinical_effective_date,
        NULL::FLOAT AS cholesterol_value, NULL::FLOAT AS triglycerides_value,
        FALSE AS is_valid_cholesterol, FALSE AS is_valid_triglycerides
    WHERE FALSE
),
actual AS (
    {% for calculation in calculations %}
    SELECT person_id, indicator_id, reporting_date, is_in_numerator
    FROM ({{ calculation }})
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)
SELECT COUNT(*) AS failure_count
FROM actual
HAVING COUNT(*) <> 4
    OR COUNT_IF(reporting_date = CURRENT_DATE() AND is_in_numerator) <> 4
