-- Pair: macros/qof_registers/calculate_osteoporosis_register.sql.
-- This live fact includes future-dated records. Its PIT pair is strict as-of
-- and derives age at the reference date where age is used.

{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        pre_hook="DROP TABLE IF EXISTS {{ this }}")
}}

/*
Osteoporosis Register fact table - one row per person.
Applies QOF osteoporosis register inclusion criteria.

Clinical Purpose:
- QOF osteoporosis register for fracture prevention
- Bone health monitoring
- DXA scanning compliance

QOF Register Criteria (Complex Pattern):
- Age 50-74 years
- AND ALL of the following:
  1. Fragility fracture after April 2012
  2. Osteoporosis diagnosis (OSTEO_COD)
  3. DXA confirmation (DXA scan OR T-score ≤ -2.5)

Includes all patients meeting clinical criteria (active, deceased, deducted).
This table provides one row per person for analytical use.
*/

WITH osteoporosis_diagnoses AS (
    SELECT
        person_id,

        -- Register inclusion dates
        MIN(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) AS earliest_diagnosis_date,
        MAX(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) AS latest_diagnosis_date,

        -- Episode counts
        COUNT(CASE WHEN is_diagnosis_code THEN 1 END)
            AS total_osteoporosis_episodes,

        -- Concept code arrays for traceability
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_code
            END
        )
            AS osteoporosis_diagnosis_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_display
            END
        )
            AS osteoporosis_diagnosis_displays,

        -- Latest observation details
        ARRAY_AGG(DISTINCT ID::VARCHAR) AS all_IDs

    FROM {{ ref('int_osteoporosis_diagnoses_all') }}
    GROUP BY person_id
),

dxa_data AS (
    SELECT
        person_id,
        COUNT(CASE WHEN is_dxa_scan_procedure THEN 1 END) > 0 AS has_dxa_scan,
        COUNT(CASE WHEN is_dxa_t_score_measurement THEN 1 END)
        > 0 AS has_dxa_t_score,
        -- Check if ANY T-score measurement is ≤ -2.5 (per QOF spec: DXA2_VAL <= -2.5)
        MAX(CASE WHEN is_dxa_t_score_measurement AND validated_t_score <= -2.5 THEN 1 ELSE 0 END) = 1 AS has_qualifying_t_score,
        -- DXA2_DAT: earliest T-score measurement where value <= -2.5
        MIN(CASE WHEN is_dxa_t_score_measurement AND validated_t_score <= -2.5 THEN clinical_effective_date END) AS earliest_qualifying_t_score_date,
        MIN(CASE WHEN is_dxa_scan_procedure THEN clinical_effective_date END)
            AS earliest_dxa_date,
        MAX(CASE WHEN is_dxa_scan_procedure THEN clinical_effective_date END)
            AS latest_dxa_date,
        MIN(
            CASE
                WHEN is_dxa_t_score_measurement THEN clinical_effective_date
            END
        ) AS earliest_dxa_t_score_date,
        MAX(
            CASE
                WHEN is_dxa_t_score_measurement THEN clinical_effective_date
            END
        ) AS latest_dxa_t_score_date,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_dxa_scan_procedure THEN concept_code END
        ) AS all_dxa_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_dxa_scan_procedure THEN concept_display END
        ) AS all_dxa_concept_displays
    FROM {{ ref('int_dxa_scans_all') }}
    GROUP BY person_id
),

fragility_fractures AS (
    SELECT
        person_id,
        COUNT(*) > 0 AS has_fragility_fracture,
        -- OSTEO1_REG: fracture on/after 2012-04-01 (int_ already filters >= 2012-04-01)
        MAX(CASE WHEN clinical_effective_date >= '2012-04-01' THEN 1 ELSE 0 END) = 1 AS has_fracture_post_2012,
        -- OSTEO2_REG: fracture on/after 2014-04-01
        MAX(CASE WHEN clinical_effective_date >= '2014-04-01' THEN 1 ELSE 0 END) = 1 AS has_fracture_post_2014,
        MIN(clinical_effective_date) AS earliest_fragility_fracture_date,
        MAX(clinical_effective_date) AS latest_fragility_fracture_date,
        COUNT(DISTINCT fracture_site) AS distinct_fracture_sites,
        COUNT(DISTINCT clinical_effective_date) AS distinct_fracture_dates,

        -- Aggregate arrays for comprehensive tracking
        ARRAY_AGG(DISTINCT concept_code)
            AS all_fragility_fracture_concept_codes,
        ARRAY_AGG(DISTINCT code_description)
            AS all_fragility_fracture_concept_displays,
        ARRAY_AGG(DISTINCT fracture_site) AS all_fracture_sites,

        -- Fracture site flags
        MAX(CASE WHEN fracture_site = 'Hip' THEN 1 ELSE 0 END)
        = 1 AS has_hip_fracture,
        MAX(CASE WHEN fracture_site = 'Wrist' THEN 1 ELSE 0 END)
        = 1 AS has_wrist_fracture,
        MAX(CASE WHEN fracture_site = 'Spine' THEN 1 ELSE 0 END)
        = 1 AS has_spine_fracture,
        MAX(CASE WHEN fracture_site = 'Humerus' THEN 1 ELSE 0 END)
        = 1 AS has_humerus_fracture

    FROM {{ ref('int_fragility_fractures_all') }}
    GROUP BY person_id
),

register_logic AS (
    SELECT
        diag.person_id,

        -- Age restriction: 50-74 years for osteoporosis register
        diag.earliest_diagnosis_date,

        -- Component 1: Fragility fracture requirement
        diag.latest_diagnosis_date,

        -- Component 2: Osteoporosis diagnosis requirement
        diag.total_osteoporosis_episodes,

        -- Component 3: DXA confirmation requirement (scan OR T-score ≤ -2.5)
        dxa.earliest_dxa_date,

        -- Additional component flags for transparency
        dxa.latest_dxa_date,
        dxa.earliest_dxa_t_score_date,

        -- Complex register inclusion: Age + ALL three components required
        dxa.latest_dxa_t_score_date,
        dxa.earliest_qualifying_t_score_date,
        frac.earliest_fragility_fracture_date,
        frac.latest_fragility_fracture_date,

        -- Clinical dates - DXA
        diag.osteoporosis_diagnosis_codes,
        diag.osteoporosis_diagnosis_displays,
        diag.all_IDs,
        dxa.all_dxa_concept_codes,
        dxa.all_dxa_concept_displays,

        -- Clinical dates - fractures
        frac.all_fragility_fracture_concept_codes,
        frac.all_fragility_fracture_concept_displays,

        -- Traceability - osteoporosis
        frac.all_fracture_sites,
        age.age,
        -- Age criteria flags for both registers
        COALESCE(age.age BETWEEN 50 AND 74, FALSE) AS meets_osteo1_age_criteria,
        COALESCE(age.age >= 75, FALSE) AS meets_osteo2_age_criteria,

        -- Fracture flags
        COALESCE(frac.has_fragility_fracture, FALSE) AS has_fragility_fracture,
        COALESCE(frac.has_fracture_post_2012, FALSE) AS has_fracture_post_2012,
        COALESCE(frac.has_fracture_post_2014, FALSE) AS has_fracture_post_2014,
        COALESCE(
            diag.earliest_diagnosis_date IS NOT NULL,
            FALSE
        ) AS has_osteoporosis_diagnosis,

        -- DXA confirmation (scan OR any T-score ≤ -2.5)
        COALESCE(
            dxa.has_dxa_scan = TRUE OR dxa.has_qualifying_t_score = TRUE,
            FALSE
        ) AS has_valid_dxa_confirmation,
        COALESCE(dxa.has_dxa_scan, FALSE) AS has_dxa_scan,
        COALESCE(dxa.has_dxa_t_score, FALSE) AS has_dxa_t_score,
        COALESCE(dxa.has_qualifying_t_score, FALSE) AS has_qualifying_t_score,

        -- Register inclusion: OSTEO1_REG OR OSTEO2_REG
        COALESCE(
            -- OSTEO1_REG: Age 50-74 + fracture post-2012 + osteoporosis dx + DXA confirmation
            (
                age.age BETWEEN 50 AND 74
                AND frac.has_fracture_post_2012 = TRUE
                AND diag.earliest_diagnosis_date IS NOT NULL
                AND (dxa.has_dxa_scan = TRUE OR dxa.has_qualifying_t_score = TRUE)
            )
            OR
            -- OSTEO2_REG: Age 75+ + fracture post-2014 + osteoporosis dx (no DXA required)
            (
                age.age >= 75
                AND frac.has_fracture_post_2014 = TRUE
                AND diag.earliest_diagnosis_date IS NOT NULL
            ),
            FALSE
        ) AS is_on_register
    FROM osteoporosis_diagnoses AS diag
    INNER JOIN {{ ref('dim_person_age') }} AS age ON diag.person_id = age.person_id
    LEFT JOIN dxa_data AS dxa ON diag.person_id = dxa.person_id
    LEFT JOIN fragility_fractures AS frac ON diag.person_id = frac.person_id
)

-- Final selection: Only individuals meeting all osteoporosis register criteria
SELECT
    person_id,
    age,
    is_on_register,

    -- Component criteria flags
    meets_osteo1_age_criteria,
    meets_osteo2_age_criteria,
    has_fragility_fracture,
    has_fracture_post_2012,
    has_fracture_post_2014,
    has_osteoporosis_diagnosis,
    has_valid_dxa_confirmation,
    has_dxa_scan,
    has_dxa_t_score,
    has_qualifying_t_score,

    -- Clinical dates - osteoporosis
    earliest_diagnosis_date,
    latest_diagnosis_date,
    total_osteoporosis_episodes,

    -- Clinical dates - DXA
    earliest_dxa_date,
    latest_dxa_date,
    earliest_dxa_t_score_date,
    latest_dxa_t_score_date,
    earliest_qualifying_t_score_date,

    -- Clinical dates - fractures
    earliest_fragility_fracture_date,
    latest_fragility_fracture_date,

    -- Traceability for audit
    osteoporosis_diagnosis_codes,
    osteoporosis_diagnosis_displays,
    all_IDs,
    all_dxa_concept_codes,
    all_dxa_concept_displays,
    all_fragility_fracture_concept_codes,
    all_fragility_fracture_concept_displays,
    all_fracture_sites
FROM register_logic
WHERE is_on_register = TRUE

ORDER BY earliest_diagnosis_date DESC, person_id ASC
