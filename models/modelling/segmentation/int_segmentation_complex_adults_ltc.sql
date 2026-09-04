{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- LTC flags for the complex adults cohort. Grain: one row per person on at
-- least one of the 16 included registers.
--
-- Included LTCs (16): AF, asthma, CHD, CKD, COPD, dementia, depression,
-- diabetes, epilepsy, heart failure, hypertension, SMI, stroke/TIA,
-- Parkinson's, anxiety, and learning disability counted only at age >= 65.
-- No NAFLD - the wider segmentation LTC list (int_segmentation_ltc_count)
-- adds it, this cohort definition does not.

SELECT
    s.person_id,
    BOOLOR_AGG(s.condition_code = 'AF') AS has_af,
    BOOLOR_AGG(s.condition_code = 'AST') AS has_asthma,
    BOOLOR_AGG(s.condition_code = 'CHD') AS has_chd,
    BOOLOR_AGG(s.condition_code = 'CKD') AS has_ckd,
    BOOLOR_AGG(s.condition_code = 'COPD') AS has_copd,
    BOOLOR_AGG(s.condition_code = 'DEM') AS has_dementia,
    BOOLOR_AGG(s.condition_code = 'DEP') AS has_depression,
    BOOLOR_AGG(s.condition_code = 'DM') AS has_diabetes,
    BOOLOR_AGG(s.condition_code = 'EP') AS has_epilepsy,
    BOOLOR_AGG(s.condition_code = 'HF') AS has_heart_failure,
    BOOLOR_AGG(s.condition_code = 'HTN') AS has_hypertension,
    BOOLOR_AGG(s.condition_code = 'SMI') AS has_smi,
    BOOLOR_AGG(s.condition_code = 'STIA') AS has_stroke_tia,
    BOOLOR_AGG(s.condition_code = 'PD') AS has_parkinsons,
    BOOLOR_AGG(s.condition_code = 'ANX') AS has_anxiety,
    BOOLOR_AGG(s.condition_code = 'LD') AS has_learning_disability,
    COUNT(DISTINCT s.condition_code) AS ltc_count
FROM {{ ref('fct_person_ltc_summary') }} AS s
INNER JOIN {{ ref('dim_person_age') }} AS a
    ON s.person_id = a.person_id
WHERE (
    s.condition_code IN (
        'AF', 'AST', 'CHD', 'CKD', 'COPD', 'DEM', 'DEP', 'DM',
        'EP', 'HF', 'HTN', 'SMI', 'STIA', 'PD', 'ANX'
    )
    -- Learning disability counts towards the LTC total only at 65+
    OR (s.condition_code = 'LD' AND a.age >= 65)
)
GROUP BY s.person_id
