{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Foot examination records and related observations, one row per person and
date. Examination evidence comes from FOOTEXAM_COD and the foot risk
classification findings in FRC_COD; FEPU_COD and FEDEC_COD mark unsuitable and
declined checks; CONABL_COD, CONABR_COD, AMPL_COD and AMPR_COD mark an absent or
amputated foot. A referral to the diabetic foot screener sits in FOOTEXAM_COD
but records a referral, not an examination: it is kept for traceability
(has_screening_referral) and never sets a checked flag. Includes ALL persons
(active, inactive, deceased) following intermediate layer principles.
*/

WITH foot_observations AS (

    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,

        -- A referral to the screener is not an examination
        LOWER(obs.code_description) LIKE 'refer to %' AS is_referral,
        obs.cluster_id IN ('FOOTEXAM_COD', 'FRC_COD')
            AND NOT LOWER(obs.code_description) LIKE 'refer to %' AS is_examination_code,

        -- NICE IND160 specifies monofilament testing, which is only part of FOOTEXAM_COD.
        obs.cluster_id = 'FOOTEXAM_COD'
            AND LOWER(obs.code_description) LIKE '%monofilament%' AS is_monofilament_test,

        -- Check if code term contains 'left' or 'right' (case insensitive)
        REGEXP_LIKE(LOWER(obs.code_description), '.*left.*') AS has_left,
        REGEXP_LIKE(LOWER(obs.code_description), '.*right.*') AS has_right,

        -- Check if code is a Townson scale and extract level
        REGEXP_LIKE(LOWER(obs.code_description), '.*townson.*scale.*level.*') AS is_townson,
        CASE
            WHEN REGEXP_LIKE(LOWER(obs.code_description), '.*townson.*scale.*level 1.*') THEN 'Level 1'
            WHEN REGEXP_LIKE(LOWER(obs.code_description), '.*townson.*scale.*level 2.*') THEN 'Level 2'
            WHEN REGEXP_LIKE(LOWER(obs.code_description), '.*townson.*scale.*level 3.*') THEN 'Level 3'
            WHEN REGEXP_LIKE(LOWER(obs.code_description), '.*townson.*scale.*level 4.*') THEN 'Level 4'
            ELSE NULL
        END AS townson_level,

        -- Extract risk level from description
        CASE
            WHEN LOWER(obs.code_description) LIKE '%low risk%' THEN 'Low'
            WHEN LOWER(obs.code_description) LIKE '%moderate risk%' THEN 'Moderate'
            WHEN LOWER(obs.code_description) LIKE '%increased risk%' THEN 'Moderate'
            WHEN LOWER(obs.code_description) LIKE '%high risk%' THEN 'High'
            WHEN LOWER(obs.code_description) LIKE '%ulcerated%' THEN 'Ulcerated'
            -- Map Townson scale levels to risk levels
            WHEN REGEXP_LIKE(LOWER(obs.code_description), '.*townson.*scale.*level 1.*') THEN 'Low'
            WHEN REGEXP_LIKE(LOWER(obs.code_description), '.*townson.*scale.*level 2.*') THEN 'Moderate'
            WHEN REGEXP_LIKE(LOWER(obs.code_description), '.*townson.*scale.*level 3.*') THEN 'High'
            WHEN REGEXP_LIKE(LOWER(obs.code_description), '.*townson.*scale.*level 4.*') THEN 'High'
            ELSE NULL
        END AS risk_level

    FROM ({{ get_observations("'FEPU_COD', 'FEDEC_COD', 'FOOTEXAM_COD', 'FRC_COD', 'CONABL_COD', 'CONABR_COD', 'AMPL_COD', 'AMPR_COD'", source='PCD') }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

-- Tag foot exam rows with no laterality keyword and no Townson as generic bilateral checks
foot_observations_tagged AS (
    SELECT
        *,
        (is_examination_code
         AND NOT has_left
         AND NOT has_right
         AND NOT is_townson) AS is_bilateral_generic
    FROM foot_observations
),

-- First aggregate foot status (amputations/absences) across all time
foot_status AS (
    SELECT
        person_id,
        MAX(CASE WHEN source_cluster_id = 'CONABL_COD' THEN TRUE ELSE FALSE END) AS left_foot_absent,
        MAX(CASE WHEN source_cluster_id = 'CONABR_COD' THEN TRUE ELSE FALSE END) AS right_foot_absent,
        MAX(CASE WHEN source_cluster_id = 'AMPL_COD' THEN TRUE ELSE FALSE END) AS left_foot_amputated,
        MAX(CASE WHEN source_cluster_id = 'AMPR_COD' THEN TRUE ELSE FALSE END) AS right_foot_amputated
    FROM foot_observations_tagged
    GROUP BY person_id
),

-- Then get the check details for each date
check_details_raw AS (
    SELECT
        person_id,
        clinical_effective_date,

        -- Check status
        MAX(CASE WHEN source_cluster_id = 'FEPU_COD' THEN TRUE ELSE FALSE END) AS is_unsuitable,
        MAX(CASE WHEN source_cluster_id = 'FEDEC_COD' THEN TRUE ELSE FALSE END) AS is_declined,

        MAX(is_monofilament_test) AS has_monofilament_test,

        -- Left foot checked: explicit left code, Townson, or generic bilateral exam code
        MAX(CASE
            WHEN is_examination_code AND (has_left OR is_townson OR is_bilateral_generic) THEN TRUE
            ELSE FALSE
        END) AS left_foot_checked,

        -- Right foot checked: explicit right code, Townson, or generic bilateral exam code
        MAX(CASE
            WHEN is_examination_code AND (has_right OR is_townson OR is_bilateral_generic) THEN TRUE
            ELSE FALSE
        END) AS right_foot_checked,

        -- Bilateral signal at the row level (Townson or generic bilateral exam code).
        -- Paired left+right rows on the same date are folded in by check_details below.
        MAX(CASE
            WHEN is_examination_code AND (is_townson OR is_bilateral_generic) THEN TRUE
            ELSE FALSE
        END) AS both_feet_checked_row_level,

        -- Risk level by foot (only populated where the code carries an explicit risk descriptor)
        MAX(CASE
            WHEN is_examination_code AND (has_left OR is_townson) THEN risk_level
            ELSE NULL
        END) AS left_foot_risk_level,

        MAX(CASE
            WHEN is_examination_code AND (has_right OR is_townson) THEN risk_level
            ELSE NULL
        END) AS right_foot_risk_level,

        -- Get Townson scale level if used
        MAX(CASE
            WHEN is_townson THEN townson_level
            ELSE NULL
        END) AS townson_scale_level,

        -- Referral to the screener recorded on this date
        MAX(is_referral) AS has_screening_referral,

        -- A risk classification finding (FRC_COD) or an examination code carrying a risk descriptor
        MAX(source_cluster_id = 'FRC_COD' OR (is_examination_code AND risk_level IS NOT NULL)) AS has_risk_classification,

        -- Collect all codes and terms for traceability
        ARRAY_AGG(DISTINCT concept_code) WITHIN GROUP (ORDER BY concept_code) AS all_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) WITHIN GROUP (ORDER BY concept_display) AS all_concept_displays,
        ARRAY_AGG(DISTINCT source_cluster_id) WITHIN GROUP (ORDER BY source_cluster_id) AS all_source_cluster_ids

    FROM foot_observations_tagged
    GROUP BY person_id, clinical_effective_date
),

-- Canonical both_feet_checked: bilateral row OR paired left+right on same date
check_details AS (
    SELECT
        person_id,
        clinical_effective_date,
        is_unsuitable,
        is_declined,
        has_monofilament_test,
        left_foot_checked,
        right_foot_checked,
        (both_feet_checked_row_level OR (left_foot_checked AND right_foot_checked)) AS both_feet_checked,
        left_foot_risk_level,
        right_foot_risk_level,
        townson_scale_level,
        has_screening_referral,
        has_risk_classification,
        all_concept_codes,
        all_concept_displays,
        all_source_cluster_ids
    FROM check_details_raw
)

-- Final selection combining check details with foot status
SELECT
    cd.person_id,
    cd.clinical_effective_date,
    cd.is_unsuitable,
    cd.is_declined,
    cd.has_monofilament_test,
    cd.left_foot_checked,
    cd.right_foot_checked,
    cd.both_feet_checked,
    fs.left_foot_absent,
    fs.right_foot_absent,
    fs.left_foot_amputated,
    fs.right_foot_amputated,
    cd.left_foot_risk_level,
    cd.right_foot_risk_level,
    cd.townson_scale_level,
    cd.has_screening_referral,
    cd.has_risk_classification,
    cd.all_concept_codes,
    cd.all_concept_displays,
    cd.all_source_cluster_ids,

    -- Check completion status
    CASE
        WHEN cd.is_unsuitable THEN 'Unsuitable'
        WHEN cd.is_declined THEN 'Declined'
        WHEN cd.both_feet_checked THEN 'Complete - Both Feet'
        WHEN cd.left_foot_checked AND (fs.right_foot_absent OR fs.right_foot_amputated) THEN 'Complete - Left Only (Right Missing)'
        WHEN cd.right_foot_checked AND (fs.left_foot_absent OR fs.left_foot_amputated) THEN 'Complete - Right Only (Left Missing)'
        WHEN cd.left_foot_checked THEN 'Partial - Left Only'
        WHEN cd.right_foot_checked THEN 'Partial - Right Only'
        WHEN cd.has_screening_referral THEN 'Referred - Not Examined'
        ELSE 'Not Done'
    END AS examination_status,

    -- Diabetes foot risk classification
    CASE
        WHEN cd.left_foot_risk_level = 'Ulcerated' OR cd.right_foot_risk_level = 'Ulcerated' THEN 'Ulcerated (High Risk)'
        WHEN cd.left_foot_risk_level = 'High' OR cd.right_foot_risk_level = 'High' THEN 'High Risk'
        WHEN cd.left_foot_risk_level = 'Moderate' OR cd.right_foot_risk_level = 'Moderate' THEN 'Moderate Risk'
        WHEN cd.left_foot_risk_level = 'Low' AND cd.right_foot_risk_level = 'Low' THEN 'Low Risk'
        WHEN cd.left_foot_risk_level = 'Low' OR cd.right_foot_risk_level = 'Low' THEN 'Low Risk'
        WHEN cd.left_foot_checked OR cd.right_foot_checked THEN 'Risk Not Specified'
        ELSE 'No Valid Examination'
    END AS diabetes_foot_risk_category

FROM check_details cd
LEFT JOIN foot_status fs ON cd.person_id = fs.person_id
ORDER BY cd.person_id, cd.clinical_effective_date DESC
