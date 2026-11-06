{{
    config(
        materialized='table',
        cluster_by=['person_id', 'observation_type'])
}}

/*
Biomarker history with repeated non-BP readings counted once per clinical day.

Unions the per-observation "_all" biomarker models into a single tall table
(person_id, observation_type, clinical_effective_date, value, unit, category)
so that serial / latest-N / trajectory questions can be answered with window
functions over a single grain. The latest-only models (int_*_latest) and the
sem_olids_observations view answer "current value" questions; this model and
sem_olids_observations_history answer "over time" questions.

Why long, not wide: the source _all models are many-rows-per-person with
divergent schemas. Co-joining them on person_id would fan out (cartesian).
Stacking them long keeps a single, safe grain and a uniform shape, at the
cost of a generic VARCHAR category per type.

Blood pressure emits two rows per reading (Systolic BP, Diastolic BP).

Count-style markers (haemoglobin, platelets, eosinophils, ALT, GGT, bilirubin) are filtered
to a non-null inferred value, non-negative and not extreme outliers, matching their _latest
models. Typed markers are included where the value is non-null.
*/

WITH bp_stage_rows AS (
    SELECT
        systolic_observation_id AS source_observation_id,
        GREATEST(
            CASE
                WHEN systolic_value >= 180 THEN 3
                WHEN COALESCE(is_home_bp_event, FALSE) OR COALESCE(is_abpm_bp_event, FALSE)
                    THEN IFF(systolic_value >= 150, 2, IFF(systolic_value >= 135, 1, 0))
                ELSE IFF(systolic_value >= 160, 2, IFF(systolic_value >= 140, 1, 0))
            END,
            CASE
                WHEN diastolic_value >= 120 THEN 3
                WHEN COALESCE(is_home_bp_event, FALSE) OR COALESCE(is_abpm_bp_event, FALSE)
                    THEN IFF(diastolic_value >= 95, 2, IFF(diastolic_value >= 85, 1, 0))
                ELSE IFF(diastolic_value >= 100, 2, IFF(diastolic_value >= 90, 1, 0))
            END
        ) AS hypertension_stage_number
    FROM {{ ref('int_blood_pressure_all') }}
    WHERE systolic_observation_id IS NOT NULL

    UNION ALL

    SELECT
        diastolic_observation_id AS source_observation_id,
        GREATEST(
            CASE
                WHEN systolic_value >= 180 THEN 3
                WHEN COALESCE(is_home_bp_event, FALSE) OR COALESCE(is_abpm_bp_event, FALSE)
                    THEN IFF(systolic_value >= 150, 2, IFF(systolic_value >= 135, 1, 0))
                ELSE IFF(systolic_value >= 160, 2, IFF(systolic_value >= 140, 1, 0))
            END,
            CASE
                WHEN diastolic_value >= 120 THEN 3
                WHEN COALESCE(is_home_bp_event, FALSE) OR COALESCE(is_abpm_bp_event, FALSE)
                    THEN IFF(diastolic_value >= 95, 2, IFF(diastolic_value >= 85, 1, 0))
                ELSE IFF(diastolic_value >= 100, 2, IFF(diastolic_value >= 90, 1, 0))
            END
        ) AS hypertension_stage_number
    FROM {{ ref('int_blood_pressure_all') }}
    WHERE diastolic_observation_id IS NOT NULL
),

blood_pressure_stages AS (
    SELECT
        source_observation_id,
        CASE MAX(hypertension_stage_number)
            WHEN 3 THEN 'Stage 3 (Severe)'
            WHEN 2 THEN 'Stage 2'
            WHEN 1 THEN 'Stage 1'
            ELSE 'Normal'
        END AS hypertension_stage
    FROM bp_stage_rows
    GROUP BY source_observation_id
),

events AS (

    -- Cardiovascular: Blood Pressure (one row each for systolic and diastolic)
    SELECT person_id, systolic_observation_id AS source_observation_id,
        'Systolic BP' AS observation_type, 'Cardiovascular' AS observation_group,
        clinical_effective_date, systolic_value::FLOAT AS value, 'mmHg' AS unit,
        (CASE WHEN is_hypertensive_range THEN 'Hypertensive range' ELSE 'Below hypertensive range' END)::VARCHAR AS category
    FROM {{ ref('int_blood_pressure_all') }}
    WHERE systolic_value IS NOT NULL

    UNION ALL
    SELECT person_id, diastolic_observation_id AS source_observation_id,
        'Diastolic BP', 'Cardiovascular',
        clinical_effective_date, diastolic_value::FLOAT, 'mmHg',
        NULL::VARCHAR
    FROM {{ ref('int_blood_pressure_all') }}
    WHERE diastolic_value IS NOT NULL

    -- Cardiovascular: Cholesterol, LDL, QRISK
    UNION ALL
    SELECT person_id, id, 'Total Cholesterol', 'Cardiovascular',
        clinical_effective_date, cholesterol_value::FLOAT, 'mmol/L', cholesterol_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_cholesterol_all', value='cholesterol_value::FLOAT', unit="'mmol/L'",
            condition='cholesterol_value IS NOT NULL') }}
    )

    UNION ALL
    SELECT person_id, id, 'LDL Cholesterol', 'Cardiovascular',
        clinical_effective_date, cholesterol_value::FLOAT, 'mmol/L', LDL_CVD_Target_Met::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_cholesterol_ldl_all', value='cholesterol_value::FLOAT', unit="'mmol/L'",
            condition='cholesterol_value IS NOT NULL') }}
    )

    UNION ALL
    SELECT person_id, id, 'HDL Cholesterol', 'Cardiovascular',
        clinical_effective_date, cholesterol_value::FLOAT, 'mmol/L', cholesterol_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_cholesterol_hdl_all', value='cholesterol_value::FLOAT', unit="'mmol/L'",
            condition='cholesterol_value IS NOT NULL') }}
    )

    UNION ALL
    SELECT person_id, id, 'Non-HDL Cholesterol', 'Cardiovascular',
        clinical_effective_date, cholesterol_value::FLOAT, 'mmol/L', cholesterol_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_cholesterol_non_hdl_all', value='cholesterol_value::FLOAT', unit="'mmol/L'",
            condition='cholesterol_value IS NOT NULL') }}
    )

    UNION ALL
    SELECT person_id, id, 'Triglycerides', 'Cardiovascular',
        clinical_effective_date, triglycerides_value::FLOAT, 'mmol/L', triglycerides_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_triglycerides_all', value='triglycerides_value::FLOAT', unit="'mmol/L'",
            condition='triglycerides_value IS NOT NULL') }}
    )

    UNION ALL
    SELECT person_id, id, 'Total Cholesterol to HDL Ratio', 'Cardiovascular',
        clinical_effective_date, cholesterol_hdl_ratio::FLOAT, 'ratio', NULL::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_cholesterol_hdl_ratio_all', value='cholesterol_hdl_ratio::FLOAT', unit="'ratio'",
            condition='cholesterol_hdl_ratio IS NOT NULL') }}
    )

    UNION ALL
    SELECT person_id, id, 'QRISK', 'Cardiovascular',
        clinical_effective_date, qrisk_score::FLOAT, '%', cvd_risk_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_qrisk_all', value='qrisk_score::FLOAT', unit="'%'",
            condition='qrisk_score IS NOT NULL') }}
    )

    -- Metabolic: HbA1c, BMI, Waist circumference
    UNION ALL
    SELECT person_id, id, 'HbA1c', 'Metabolic',
        clinical_effective_date, hba1c_ifcc::FLOAT, 'mmol/mol', hba1c_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_hba1c_all', value='hba1c_ifcc::FLOAT', unit="'mmol/mol'",
            condition='hba1c_ifcc IS NOT NULL') }}
    )

    UNION ALL
    SELECT person_id, id, 'BMI', 'Metabolic',
        clinical_effective_date, bmi_value::FLOAT, 'kg/m2', bmi_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_bmi_all', value='bmi_value::FLOAT', unit="'kg/m2'",
            condition='bmi_value IS NOT NULL', extra_lookup_cluster='WEIGHT') }}
    )

    UNION ALL
    SELECT person_id, id, 'Waist Circumference', 'Metabolic',
        clinical_effective_date, waist_circumference_value::FLOAT, 'cm', waist_risk_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_waist_circumference_all', value='waist_circumference_value::FLOAT', unit="'cm'",
            condition='waist_circumference_value IS NOT NULL') }}
    )

    -- Renal: eGFR, Creatinine, Urine ACR
    UNION ALL
    SELECT person_id, id, 'eGFR', 'Renal',
        clinical_effective_date, egfr_value::FLOAT, 'mL/min/1.73m2', ckd_stage::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_egfr_all', value='egfr_value::FLOAT', unit="'mL/min/1.73m2'",
            condition='egfr_value IS NOT NULL') }}
    )

    UNION ALL
    SELECT person_id, id, 'Creatinine', 'Renal',
        clinical_effective_date, creatinine_value::FLOAT, 'umol/L', creatinine_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_creatinine_all', value='creatinine_value::FLOAT', unit="'umol/L'",
            condition='creatinine_value IS NOT NULL') }}
    )

    UNION ALL
    SELECT person_id, id, 'Urine ACR', 'Renal',
        clinical_effective_date, acr_value::FLOAT, 'mg/mmol', acr_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_urine_acr_all', value='acr_value::FLOAT', unit="'mg/mmol'",
            condition='acr_value IS NOT NULL') }}
    )

    -- Liver: ALT, GGT, Bilirubin
    UNION ALL
    SELECT person_id, id, 'ALT', 'Liver',
        clinical_effective_date, inferred_value::FLOAT, inferred_unit::VARCHAR, alt_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_alt_all', value='inferred_value::FLOAT', unit="inferred_unit::VARCHAR",
            condition='NOT is_negative AND NOT is_extreme_outlier AND inferred_value IS NOT NULL') }}
    )

    UNION ALL
    SELECT person_id, id, 'GGT', 'Liver',
        clinical_effective_date, inferred_value::FLOAT, inferred_unit::VARCHAR, ggt_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_ggt_all', value='inferred_value::FLOAT', unit="inferred_unit::VARCHAR",
            condition='NOT is_negative AND NOT is_extreme_outlier AND inferred_value IS NOT NULL') }}
    )

    UNION ALL
    SELECT person_id, id, 'Bilirubin', 'Liver',
        clinical_effective_date, inferred_value::FLOAT, inferred_unit::VARCHAR, bilirubin_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_bilirubin_all', value='inferred_value::FLOAT', unit="inferred_unit::VARCHAR",
            condition='NOT is_negative AND NOT is_extreme_outlier AND inferred_value IS NOT NULL') }}
    )

    -- Haematology: Haemoglobin, Platelets, Eosinophils
    UNION ALL
    SELECT person_id, id, 'Haemoglobin', 'Haematology',
        clinical_effective_date, inferred_value::FLOAT, inferred_unit::VARCHAR, haemoglobin_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_haemoglobin_all', value='inferred_value::FLOAT', unit="inferred_unit::VARCHAR",
            condition='NOT is_negative AND NOT is_extreme_outlier AND NOT is_out_of_range AND inferred_value IS NOT NULL') }}
    )

    UNION ALL
    SELECT person_id, id, 'Platelets', 'Haematology',
        clinical_effective_date, inferred_value::FLOAT, inferred_unit::VARCHAR, platelets_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_platelets_all', value='inferred_value::FLOAT', unit="inferred_unit::VARCHAR",
            condition='NOT is_negative AND NOT is_extreme_outlier AND inferred_value IS NOT NULL') }}
    )

    UNION ALL
    SELECT person_id, id, 'Eosinophils', 'Haematology',
        clinical_effective_date, inferred_value::FLOAT, inferred_unit::VARCHAR, eosinophil_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_eosinophil_count', value='inferred_value::FLOAT', unit="inferred_unit::VARCHAR",
            condition='NOT is_negative AND NOT is_extreme_outlier AND inferred_value IS NOT NULL') }}
    )

    -- Frailty: explicitly coded eFI/eFI2 scores and Rockwood assessments
    UNION ALL
    SELECT person_id, id, 'Electronic Frailty Index (eFI)', 'Frailty',
        clinical_effective_date, efi_value::FLOAT, 'score', efi_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_efi_all', value='efi_value::FLOAT', unit="'score'",
            condition='efi_value IS NOT NULL') }}
    )

    UNION ALL
    SELECT person_id, id, 'Rockwood Frailty Scale', 'Frailty',
        clinical_effective_date, rockwood_score::FLOAT, 'score', frailty_category::VARCHAR
    FROM (
        {{ distinct_observation_history_readings(
            'int_rockwood_all', value='rockwood_score::FLOAT', unit="'score'",
            condition='rockwood_score IS NOT NULL') }}
    )
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'events.person_id', 'events.observation_type', 'events.clinical_effective_date',
        'events.source_observation_id', 'events.value'
    ]) }} AS observation_event_id,
    events.person_id,
    events.observation_type,
    events.observation_group,
    events.clinical_effective_date,
    events.value,
    events.unit,
    events.category,
    blood_pressure_stages.hypertension_stage,
    events.source_observation_id
FROM events
LEFT JOIN blood_pressure_stages
    ON events.source_observation_id = blood_pressure_stages.source_observation_id
WHERE events.clinical_effective_date <= CURRENT_DATE
