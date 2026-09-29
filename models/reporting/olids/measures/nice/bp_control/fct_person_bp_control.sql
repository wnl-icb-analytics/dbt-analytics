{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Blood Pressure Control Status
-- Applies patient-specific NG136 BP targets with priority ranking (most stringent first).
-- Targets come from the bp_thresholds seed, which holds both CLINIC and HBPM_ABPM
-- variants for each TARGET_UPPER rule (HBPM_ABPM is clinic -5 mmHg per NG136 rec 1.4.22).
-- The join below selects the variant matching the latest reading source so control is
-- scored on the same scale as the reading.
-- Also produces NG136 hypertension staging using the measurement-appropriate thresholds.

WITH latest_bp AS (
    -- Get most recent BP event (paired systolic/diastolic) for each person
    SELECT
        person_id,
        clinical_effective_date,
        systolic_value,
        diastolic_value,
        is_home_bp_event,
        is_abpm_bp_event
    FROM {{ ref('int_blood_pressure_latest') }}
),

patient_characteristics AS (
    -- Gather key patient characteristics for BP threshold determination
    SELECT
        bp.person_id,
        bp.clinical_effective_date AS latest_bp_date,
        bp.systolic_value AS latest_systolic_value,
        bp.diastolic_value AS latest_diastolic_value,
        bp.is_home_bp_event,
        bp.is_abpm_bp_event,
        -- Reading source used to pick CLINIC vs HBPM_ABPM threshold variant
        CASE
            WHEN COALESCE(bp.is_home_bp_event, FALSE) OR COALESCE(bp.is_abpm_bp_event, FALSE) THEN 'HBPM_ABPM'
            ELSE 'CLINIC'
        END AS reading_measurement_context,
        age.age,

        -- Diabetes status (Type 2 specifically for BP thresholds)
        dm.diabetes_type,
        acr.acr_value AS latest_acr_value,

        -- CKD status and latest ACR for threshold determination
        COALESCE(dm.is_on_register, FALSE) AS is_on_dm_register,
        COALESCE(ckd.person_id IS NOT NULL, FALSE) AS has_ckd,

        -- Hypertension diagnosis status
        COALESCE(htn.is_on_register, FALSE) AS is_diagnosed_htn,

        -- Other CVD registers requiring annual BP monitoring
        COALESCE(chd.is_on_register, FALSE) AS has_chd,
        COALESCE(stroke_tia.is_on_register, FALSE) AS has_stroke_tia,
        COALESCE(pad.is_on_register, FALSE) AS has_pad

    FROM latest_bp AS bp
    INNER JOIN
        {{ ref('dim_person_age') }} AS age
        ON bp.person_id = age.person_id
    LEFT JOIN
        {{ ref('fct_person_diabetes_register') }} AS dm
        ON bp.person_id = dm.person_id
    LEFT JOIN
        {{ ref('fct_person_ckd_register') }} AS ckd
        ON bp.person_id = ckd.person_id
    LEFT JOIN
        {{ ref('fct_person_hypertension_register') }} AS htn
        ON bp.person_id = htn.person_id
    LEFT JOIN
        {{ ref('fct_person_chd_register') }} AS chd
        ON bp.person_id = chd.person_id
    LEFT JOIN
        {{ ref('fct_person_stroke_tia_register') }} AS stroke_tia
        ON bp.person_id = stroke_tia.person_id
    LEFT JOIN
        {{ ref('fct_person_pad_register') }} AS pad
        ON bp.person_id = pad.person_id
    LEFT JOIN
        {{ ref('int_urine_acr_latest') }} AS acr
        ON bp.person_id = acr.person_id
),

ranked_thresholds AS (
    -- Apply BP thresholds with priority ranking (most stringent first).
    -- Also filter to the measurement_context that matches the reading source
    -- (CLINIC vs HBPM_ABPM) so HBPM/ABPM readings are scored against the
    -- NG136 -5 mmHg variant rather than the clinic target.
    SELECT
        pc.*,
        thr.threshold_rule_id,
        thr.patient_group,
        thr.measurement_context AS applied_measurement_context,
        thr.systolic_threshold,
        thr.diastolic_threshold,

        -- Priority ranking (lowest number = highest priority/most stringent)
        CASE thr.patient_group
            WHEN 'CKD_ACR_GE_70' THEN 1  -- Most stringent: CKD with high ACR
            WHEN 'T2DM' THEN 2           -- T2DM under 80
            WHEN 'CKD' THEN 3            -- General CKD under 80
            WHEN 'AGE_GE_80' THEN 4      -- Age 80+
            WHEN 'AGE_LT_80' THEN 5      -- Default: Age under 80
            ELSE 99
        END AS priority_rank

    FROM patient_characteristics AS pc
    INNER JOIN {{ ref('bp_thresholds') }} AS thr
        ON thr.measurement_context = pc.reading_measurement_context
        AND (
            -- Age-based thresholds (everyone gets one of these)
            (thr.patient_group = 'AGE_LT_80' AND pc.age < 80)
            OR (thr.patient_group = 'AGE_GE_80' AND pc.age >= 80)

            -- T2DM patients under 80 get more stringent threshold
            OR (
                thr.patient_group = 'T2DM' AND pc.is_on_dm_register
                AND pc.diabetes_type = 'Type 2' AND pc.age < 80
            )

            -- CKD patients under 80 (general CKD threshold)
            OR (thr.patient_group = 'CKD' AND pc.has_ckd AND pc.age < 80)

            -- CKD patients under 80 with high ACR (≥70) get most stringent threshold
            OR (
                thr.patient_group = 'CKD_ACR_GE_70' AND pc.has_ckd
                AND pc.latest_acr_value >= 70 AND pc.age < 80
            )
        )
    WHERE
        thr.threshold_type = 'TARGET_UPPER'
        AND thr.operator = 'BELOW'
    -- threshold_rule_id is unique per seed row, giving a deterministic order when
    -- priority_rank ties (it shouldn't tie today — patient_groups map 1:1 to rank —
    -- but the belt-and-braces tie-break guards against future threshold additions)
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY pc.person_id
            ORDER BY priority_rank ASC, thr.threshold_rule_id ASC
        ) = 1
),

with_staging AS (
    -- Add NG136 hypertension staging. The ranked_thresholds join has already picked the
    -- measurement-appropriate TARGET_UPPER row (CLINIC or HBPM_ABPM -5 mmHg variant), so
    -- rt.systolic_threshold / rt.diastolic_threshold are the values to compare against.
    -- Staging uses the same canonical ambulatory flag (applied_measurement_context) as
    -- the control comparison so both paths share one source of truth for reading source.
    SELECT
        rt.*,
        rt.applied_measurement_context = 'HBPM_ABPM' AS is_ambulatory_reading,

        -- Systolic stage (using measurement-appropriate thresholds)
        CASE
            WHEN rt.latest_systolic_value >= 180 THEN 3  -- Stage 3: ≥180 (same for both)
            WHEN rt.applied_measurement_context = 'HBPM_ABPM' THEN
                CASE
                    WHEN rt.latest_systolic_value >= 150 THEN 2  -- Stage 2 ABPM/HBPM: ≥150
                    WHEN rt.latest_systolic_value >= 135 THEN 1  -- Stage 1 ABPM/HBPM: ≥135
                    ELSE 0  -- Normal
                END
            ELSE
                CASE
                    WHEN rt.latest_systolic_value >= 160 THEN 2  -- Stage 2 Clinic: ≥160
                    WHEN rt.latest_systolic_value >= 140 THEN 1  -- Stage 1 Clinic: ≥140
                    ELSE 0  -- Normal
                END
        END AS systolic_stage,

        -- Diastolic stage (using measurement-appropriate thresholds)
        CASE
            WHEN rt.latest_diastolic_value >= 120 THEN 3  -- Stage 3: ≥120 (same for both)
            WHEN rt.applied_measurement_context = 'HBPM_ABPM' THEN
                CASE
                    WHEN rt.latest_diastolic_value >= 95 THEN 2  -- Stage 2 ABPM/HBPM: ≥95
                    WHEN rt.latest_diastolic_value >= 85 THEN 1  -- Stage 1 ABPM/HBPM: ≥85
                    ELSE 0  -- Normal
                END
            ELSE
                CASE
                    WHEN rt.latest_diastolic_value >= 100 THEN 2  -- Stage 2 Clinic: ≥100
                    WHEN rt.latest_diastolic_value >= 90 THEN 1   -- Stage 1 Clinic: ≥90
                    ELSE 0  -- Normal
                END
        END AS diastolic_stage

    FROM ranked_thresholds AS rt
),

with_monitoring_interval AS (
    SELECT
        *,
        CASE
            WHEN
                (is_on_dm_register AND diabetes_type = 'Type 2')
                OR has_ckd
                OR is_diagnosed_htn
                OR has_chd
                OR has_stroke_tia
                OR has_pad
                THEN 12
            WHEN age >= 40 THEN 60
        END AS recommended_monitoring_interval_months
    FROM with_staging
)

-- Final output: BP control status with applied thresholds, staging, and timeliness
SELECT
    ws.person_id,

    -- Latest BP event details
    ws.latest_bp_date,
    ws.latest_systolic_value,
    ws.latest_diastolic_value,
    ws.is_home_bp_event,
    ws.is_abpm_bp_event,

    -- Patient characteristics
    ws.age,
    ws.has_ckd,
    ws.is_diagnosed_htn,
    ws.has_chd,
    ws.has_stroke_tia,
    ws.has_pad,
    ws.latest_acr_value,
    ws.threshold_rule_id AS applied_threshold_rule_id,

    -- Applied threshold details — the row selected from bp_thresholds already matches
    -- the latest reading's measurement context (CLINIC or HBPM_ABPM -5 mmHg variant)
    ws.patient_group AS applied_patient_group,
    ws.applied_measurement_context,
    ws.systolic_threshold AS applied_systolic_threshold,
    ws.diastolic_threshold AS applied_diastolic_threshold,
    (ws.is_on_dm_register AND ws.diabetes_type = 'Type 2') AS has_t2dm,

    -- BP control: compare against the measurement-appropriate target
    COALESCE(
        ws.latest_systolic_value IS NOT NULL
        AND ws.latest_systolic_value < ws.systolic_threshold,
        FALSE
    ) AS is_systolic_controlled,

    COALESCE(
        ws.latest_diastolic_value IS NOT NULL
        AND ws.latest_diastolic_value < ws.diastolic_threshold,
        FALSE
    ) AS is_diastolic_controlled,

    -- Overall control: both systolic AND diastolic must be controlled
    COALESCE((
        ws.latest_systolic_value IS NOT NULL
        AND ws.latest_systolic_value < ws.systolic_threshold
    )
    AND (
        ws.latest_diastolic_value IS NOT NULL
        AND ws.latest_diastolic_value < ws.diastolic_threshold
    ), FALSE) AS is_overall_bp_controlled,

    -- Hypertension staging (NICE NG136 diagnostic thresholds)
    -- Uses highest stage between systolic and diastolic
    GREATEST(ws.systolic_stage, ws.diastolic_stage) AS hypertension_stage_number,

    CASE GREATEST(ws.systolic_stage, ws.diastolic_stage)
        WHEN 3 THEN 'Stage 3 (Severe)'
        WHEN 2 THEN 'Stage 2'
        WHEN 1 THEN 'Stage 1'
        ELSE 'Normal'
    END AS hypertension_stage,

    -- Which measurement type determined the staging thresholds
    CASE
        WHEN ws.is_ambulatory_reading THEN 'ABPM/HBPM'
        ELSE 'Clinic'
    END AS staging_threshold_basis,

    -- Case-finding helper: elevated BP but not on hypertension register
    CASE
        WHEN GREATEST(ws.systolic_stage, ws.diastolic_stage) >= 1
            AND NOT ws.is_diagnosed_htn
        THEN TRUE
        ELSE FALSE
    END AS is_case_finding_candidate,

    -- BP reading timeliness assessment
    DATEDIFF(MONTH, ws.latest_bp_date, CURRENT_DATE())
        AS latest_bp_reading_age_months,

    -- Recommended monitoring interval with description
    ws.recommended_monitoring_interval_months,
    CASE ws.recommended_monitoring_interval_months
        WHEN 12 THEN '12 months'
        WHEN 60 THEN '5 years'
        ELSE 'No routine screening'
    END AS recommended_monitoring_interval,

    -- Risk-based timeliness using the shared recommended interval
    CASE
        WHEN ws.recommended_monitoring_interval_months IS NULL THEN NULL
        ELSE COALESCE(
            ws.latest_bp_date >= DATEADD(
                MONTH,
                -ws.recommended_monitoring_interval_months,
                CURRENT_DATE()
            ),
            FALSE
        )
    END AS is_latest_bp_within_recommended_interval

FROM with_monitoring_interval AS ws
ORDER BY ws.person_id
