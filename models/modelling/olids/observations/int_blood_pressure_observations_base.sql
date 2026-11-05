{{
    config(
        materialized='table',
        tags=['intermediate', 'clinical', 'blood_pressure', 'incremental'],
        cluster_by=['person_id', 'effective_date'])
}}

/*
Base Blood Pressure Observations
================================================
Raw BP observations with row classification flags.
Shared foundation for int_blood_pressure_all and dq_blood_pressure_issues.

Note: Future date handling (clinical_effective_date > date_recorded) is applied
in the get_observations macro, benefiting all models that use it.
*/

WITH concept_setting AS (
    SELECT
        code AS concept_code,
        BOOLOR_AGG(UPPER(cluster_id) = 'ABPM_COD') AS is_abpm_concept,
        BOOLOR_AGG(UPPER(cluster_id) IN ('HOMEBP_COD', 'HOMEAMBBP_COD')) AS is_home_concept
    FROM {{ ref('stg_reference_combined_codesets') }}
    WHERE UPPER(cluster_id) IN ('ABPM_COD', 'HOMEBP_COD', 'HOMEAMBBP_COD')
    GROUP BY code
),

base_observations AS (
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,  -- Already corrected by macro if needed
        obs.clinical_effective_date_raw,  -- Original value from macro
        obs.date_recorded,
        obs.result_value,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        -- Parent observation links systolic + diastolic of the same reading.
        -- Used downstream to pair within a reading rather than by date.
        src.parent_observation_id
    FROM ({{ get_observations("'BP_COD', 'SYSBP_COD', 'DIASBP_COD', 'HOMEAMBBP_COD', 'ABPM_COD', 'HOMEBP_COD'") }}) obs
    LEFT JOIN {{ ref('stg_olids_observation') }} src
        ON obs.id = src.id
    WHERE obs.result_value IS NOT NULL
      AND obs.person_id IS NOT NULL
)

SELECT
    id,
    person_id,
    clinical_effective_date AS effective_date,
    
    -- Keep original values for audit/DQ
    clinical_effective_date_raw,
    date_recorded,
    
    result_value,
    'mmHg' AS result_unit_display,
    b.concept_code,
    concept_display,
    source_cluster_id,
    parent_observation_id,
    CASE WHEN cs.is_abpm_concept THEN 'ABPM'
        WHEN cs.is_home_concept THEN 'HOME'
        ELSE 'CLINIC'
    END AS bp_context_group,
    CASE bp_context_group
        WHEN 'ABPM' THEN CASE
            WHEN b.concept_code IN ('314449000','314462001','314464000','314465004') THEN 'ABPM_24H'
            WHEN b.concept_code IN ('314446007','314461008') THEN 'ABPM_DAY'
            WHEN b.concept_code IN ('314445006','314460009') THEN 'ABPM_NIGHT'
            WHEN b.concept_code IN ('314451001','314456006','314447003','314438006','314442009',
                                  '314441002','723236006','314455005','314454009') THEN 'ABPM_MIN_MAX'
            ELSE 'ABPM_UNSPECIFIED' END
        WHEN 'HOME' THEN CASE
            WHEN b.concept_code IN ('413606001','413605002','1285115008') THEN 'HOME_AVERAGE'
            WHEN b.concept_code IN ('1162737008','1162735000','413153004') THEN 'HOME_SELF_REPORTED'
            ELSE 'HOME_UNSPECIFIED' END
        ELSE CASE
            WHEN b.concept_code IN ('407554009','407555005','163035008') THEN 'CLINIC_SITTING'
            WHEN b.concept_code IN ('400974009','400975005','163034007') THEN 'CLINIC_STANDING'
            WHEN b.concept_code IN ('407556006','407557002','163033001') THEN 'CLINIC_LYING'
            ELSE 'CLINIC_UNSPECIFIED' END
    END AS bp_context,

    -- BP type classification flags
    (source_cluster_id = 'SYSBP_COD' OR
     (source_cluster_id = 'BP_COD' AND concept_display ILIKE '%systolic%')) AS is_systolic_row,
    
    (source_cluster_id = 'DIASBP_COD' OR
     (source_cluster_id = 'BP_COD' AND concept_display ILIKE '%diastolic%')) AS is_diastolic_row,
    
    -- Clinical context flags
    (source_cluster_id IN ('HOMEBP_COD', 'HOMEAMBBP_COD')) AS is_home_bp_row,
    (source_cluster_id = 'ABPM_COD') AS is_abpm_bp_row,
    
    -- DQ flags for downstream filtering
    (clinical_effective_date_raw > date_recorded) AS had_future_date,
    (clinical_effective_date_raw IS NULL) AS had_null_date

FROM base_observations b
LEFT JOIN concept_setting cs ON b.concept_code = cs.concept_code
