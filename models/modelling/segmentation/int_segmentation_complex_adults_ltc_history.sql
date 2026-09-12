{{
    config(
        materialized='table',
        cluster_by=['end_date', 'person_id'],
        tags=['monthly-full']
    )
}}

-- Monthly LTC flags for the complex-adults criteria.

WITH qualifying_registers AS (
    SELECT h.*
    FROM {{ ref('int_segmentation_ltc_history') }} AS h
    INNER JOIN {{ ref('int_segmentation_person_month_spine') }} AS pm
        ON h.person_id = pm.person_id
        AND h.end_date = pm.month_end_date
    WHERE
        pm.age >= 18
        AND h.condition_code != 'NAFLD'
        AND (h.condition_code != 'LD' OR pm.age >= 65)
)

SELECT
    person_id,
    end_date,
    BOOLOR_AGG(condition_code = 'AF') AS has_af,
    BOOLOR_AGG(condition_code = 'AST') AS has_asthma,
    BOOLOR_AGG(condition_code = 'CHD') AS has_chd,
    BOOLOR_AGG(condition_code = 'CKD') AS has_ckd,
    BOOLOR_AGG(condition_code = 'COPD') AS has_copd,
    BOOLOR_AGG(condition_code = 'DEM') AS has_dementia,
    BOOLOR_AGG(condition_code = 'DEP') AS has_depression,
    BOOLOR_AGG(condition_code = 'DM') AS has_diabetes,
    BOOLOR_AGG(condition_code = 'EP') AS has_epilepsy,
    BOOLOR_AGG(condition_code = 'HF') AS has_heart_failure,
    BOOLOR_AGG(condition_code = 'HTN') AS has_hypertension,
    BOOLOR_AGG(condition_code = 'SMI') AS has_smi,
    BOOLOR_AGG(condition_code = 'STIA') AS has_stroke_tia,
    BOOLOR_AGG(condition_code = 'PD') AS has_parkinsons,
    BOOLOR_AGG(condition_code = 'ANX') AS has_anxiety,
    BOOLOR_AGG(condition_code = 'LD') AS has_learning_disability,
    COUNT(DISTINCT condition_code) AS ltc_count
FROM qualifying_registers
GROUP BY person_id, end_date
