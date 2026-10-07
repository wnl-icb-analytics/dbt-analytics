{% macro calculate_nice_ltc_population(reference='current') %}
{#-
    Calculate NICE LTC flags, register counts and diagnosis anchors at each date.
    Args: reference is current or by_month.
    Returns: one active, non-test candidate person/date, with register and lifetime
             history flags, child depression, SMI activity and independent lithium.
-#}
-- NICE LTC population inputs, one candidate person and reference date.
WITH reference_dates AS (
    SELECT reporting_date AS reference_date
    FROM ({{ nice_reference_dates(reference) }})
),

conditions AS (
    SELECT
        person_id,
        reporting_date,
        COUNT(DISTINCT condition_code) AS ltc_count,
        BOOLOR_AGG(condition_code = 'CHD') AS has_chd,
        BOOLOR_AGG(condition_code = 'PAD') AS has_pad,
        BOOLOR_AGG(condition_code = 'STIA') AS has_stroke_tia,
        BOOLOR_AGG(condition_code = 'HTN') AS has_hypertension,
        BOOLOR_AGG(condition_code = 'DM') AS has_diabetes,
        BOOLOR_AGG(condition_code = 'NDH') AS has_ndh,
        BOOLOR_AGG(condition_code = 'COPD') AS has_copd,
        BOOLOR_AGG(condition_code = 'CKD') AS has_ckd,
        BOOLOR_AGG(condition_code IN ('AST', 'CYP_AST')) AS has_asthma,
        BOOLOR_AGG(condition_code = 'AF') AS has_atrial_fibrillation,
        BOOLOR_AGG(condition_code = 'HF') AS has_heart_failure,
        BOOLOR_AGG(condition_code = 'DEM') AS has_dementia,
        BOOLOR_AGG(condition_code = 'DEP') AS has_depression,
        BOOLOR_AGG(condition_code = 'ANX') AS has_anxiety,
        BOOLOR_AGG(condition_code = 'SMI') AS has_smi,
        BOOLOR_AGG(condition_code IN ('LD', 'LD_U14')) AS has_learning_disability,
        BOOLOR_AGG(condition_code = 'RA') AS has_rheumatoid_arthritis,
        BOOLOR_AGG(condition_code = 'THY') AS has_hypothyroidism,
        BOOLOR_AGG(condition_code = 'CAN') AS has_cancer,
        MAX(CASE WHEN condition_code = 'CAN' THEN latest_diagnosis_date::DATE END) AS latest_cancer_diagnosis_date,
        MIN(CASE WHEN condition_code = 'SMI' THEN earliest_diagnosis_date::DATE END) AS earliest_smi_diagnosis_date,
        MIN(CASE WHEN condition_code = 'DEM' THEN earliest_diagnosis_date::DATE END) AS earliest_dementia_diagnosis_date,
        MIN(CASE WHEN condition_code = 'DM' THEN earliest_diagnosis_date::DATE END) AS earliest_diabetes_diagnosis_date,
        COUNT(DISTINCT CASE
            WHEN condition_code = 'CAN' THEN 'CANCER'
            WHEN condition_code IN ('CHD', 'AF', 'HF', 'HTN', 'STIA', 'PAD') THEN 'CIRCULATORY'
            WHEN condition_code = 'DM' THEN 'DIABETES'
            WHEN condition_code = 'CLD' THEN 'DIGESTIVE'
            WHEN condition_code IN ('LD', 'LD_U14') THEN 'LEARNING_DISABILITY'
            WHEN condition_code IN ('ANX', 'DEP', 'DEM')
                OR (condition_code = 'SMI' AND earliest_diagnosis_date IS NOT NULL) THEN 'MENTAL_HEALTH'
            WHEN condition_code = 'RA' THEN 'MUSCULOSKELETAL'
            WHEN condition_code IN ('EP', 'MS', 'PD') THEN 'NEUROLOGICAL'
            WHEN condition_code = 'CKD' THEN 'RENAL'
            WHEN condition_code IN ('AST', 'CYP_AST', 'COPD') THEN 'RESPIRATORY'
        END) AS register_cluster_count,
        MIN(CASE WHEN condition_code IN ('CHD', 'PAD', 'STIA', 'HTN', 'DM', 'COPD', 'CKD', 'AST', 'CYP_AST')
            THEN earliest_diagnosis_date::DATE END) AS earliest_smoking_ltc_diagnosis_date,
        MIN(CASE WHEN condition_code IN ('CHD', 'PAD', 'STIA', 'HTN', 'DM', 'COPD', 'CKD', 'AST', 'CYP_AST', 'SMI')
            THEN earliest_diagnosis_date::DATE END) AS earliest_smoking_smi_ltc_diagnosis_date,
        MIN(CASE WHEN condition_code = 'HTN' THEN earliest_diagnosis_date::DATE END) AS earliest_hypertension_date,
        MIN(CASE WHEN condition_code IN ('DEP', 'ANX') THEN earliest_diagnosis_date::DATE END) AS earliest_depression_anxiety_date
    FROM ({{ nice_ltc_summary(reference) }})
    GROUP BY person_id, reporting_date

),

depression_by_known_date AS (
    -- Preserve clinical timestamp ordering, including same-time resolution priority.
    SELECT
        person_id,
        {{ ltc_known_date('clinical_effective_date', 'date_recorded') }} AS known_date,
        MIN(IFF(is_diagnosis_code AND is_first_or_new_episode, clinical_effective_date, NULL)) AS first_diagnosis,
        MAX(IFF(is_diagnosis_code AND is_first_or_new_episode, clinical_effective_date, NULL)) AS latest_diagnosis,
        MAX(IFF(is_resolved_code, clinical_effective_date, NULL)) AS latest_resolution
    FROM {{ ref('int_depression_diagnoses_all') }}
    WHERE (is_diagnosis_code AND is_first_or_new_episode) OR is_resolved_code
    GROUP BY person_id, known_date
),

depression_running AS (
    -- A late-recorded earlier episode can change the earliest anchor, not the latest state.
    SELECT
        person_id,
        known_date,
        MIN(first_diagnosis) OVER (
            PARTITION BY person_id ORDER BY known_date ROWS UNBOUNDED PRECEDING
        ) AS first_diagnosis,
        MAX(latest_diagnosis) OVER (
            PARTITION BY person_id ORDER BY known_date ROWS UNBOUNDED PRECEDING
        ) AS latest_diagnosis,
        MAX(latest_resolution) OVER (
            PARTITION BY person_id ORDER BY known_date ROWS UNBOUNDED PRECEDING
        ) AS latest_resolution
    FROM depression_by_known_date
),

population AS (
    {{ nice_reference_population(reference) }}
),

depression AS (
    SELECT
        population.person_id,
        population.reporting_date,
        state.latest_diagnosis::DATE AS latest_new_depression_diagnosis_date,
        IFF(population.age BETWEEN 10 AND 17
            AND state.latest_diagnosis > COALESCE(state.latest_resolution, '1900-01-01'),
            state.first_diagnosis::DATE, NULL) AS earliest_child_depression_date
    FROM population
    ASOF JOIN depression_running AS state
        MATCH_CONDITION (population.reporting_date >= state.known_date)
        ON population.person_id = state.person_id
),

smi AS (
    {{ nice_register('SMI', reference) }}
),

frailty AS (
    {{ nice_register('FRAIL', reference) }}
),

active_lithium AS (
    {{ calculate_nice_lithium_therapy('reference_dates') }}
),

alcohol_disorder AS (
    SELECT
        person_id,
        MIN(clinical_effective_date::DATE) AS first_date
    FROM {{ ref('int_alcohol_misuse_disorders') }}
    GROUP BY person_id
),

nice_alcohol_disorder AS (
    SELECT
        person_id,
        MIN(clinical_effective_date::DATE) AS first_date
    FROM {{ ref('int_nice_alcohol_misuse_all') }}
    GROUP BY person_id
),

dyslipidaemia AS (
    SELECT
        person_id,
        MIN(clinical_effective_date::DATE) AS first_date
    FROM {{ ref('int_dyslipidaemia_diagnoses_all') }}
    GROUP BY person_id
),

sleep_apnoea AS (
    SELECT
        person_id,
        MIN(clinical_effective_date::DATE) AS first_date
    FROM {{ ref('int_obstructive_sleep_apnoea_diagnoses_all') }}
    GROUP BY person_id
),

cvd AS (
    SELECT
        person_id,
        reporting_date,
        earliest_cvd_diagnosis_date
    FROM {{ nice_ref('int_cvd_secondary_prevention_population', reference) }} AS profile
)

SELECT
    population.person_id,
    population.reporting_date,
    population.birth_date_approx,
    COALESCE(c.ltc_count, 0) AS ltc_count,
    -- Alcohol history contributes only when the register mental-health category is absent.
    COALESCE(c.register_cluster_count, 0)
        + IFF(alcohol_disorder.first_date <= population.reporting_date
            AND NOT COALESCE(c.has_anxiety OR c.has_depression OR c.has_dementia
                OR c.earliest_smi_diagnosis_date IS NOT NULL, FALSE), 1, 0)
        AS multimorbidity_cluster_count,
    COALESCE(c.has_chd, FALSE) AS has_chd,
    COALESCE(c.has_pad, FALSE) AS has_pad,
    COALESCE(c.has_stroke_tia, FALSE) AS has_stroke_tia,
    COALESCE(c.has_hypertension, FALSE) AS has_hypertension,
    COALESCE(c.has_diabetes, FALSE) AS has_diabetes,
    COALESCE(c.has_ndh, FALSE) AS has_ndh,
    COALESCE(c.has_copd, FALSE) AS has_copd,
    COALESCE(c.has_ckd, FALSE) AS has_ckd,
    COALESCE(c.has_asthma, FALSE) AS has_asthma,
    COALESCE(c.has_atrial_fibrillation, FALSE) AS has_atrial_fibrillation,
    COALESCE(c.has_heart_failure, FALSE) AS has_heart_failure,
    COALESCE(c.has_dementia, FALSE) AS has_dementia,
    COALESCE(c.has_depression, FALSE) AS has_depression,
    COALESCE(c.has_anxiety, FALSE) AS has_anxiety,
    COALESCE(c.has_smi, FALSE) AS has_smi,
    COALESCE(c.has_learning_disability, FALSE) AS has_learning_disability,
    COALESCE(c.has_rheumatoid_arthritis, FALSE) AS has_rheumatoid_arthritis,
    COALESCE(c.has_hypothyroidism, FALSE) AS has_hypothyroidism,
    COALESCE(c.has_cancer, FALSE) AS has_cancer,
    c.latest_cancer_diagnosis_date,
    c.earliest_smi_diagnosis_date,
    c.earliest_dementia_diagnosis_date,
    c.earliest_diabetes_diagnosis_date,
    cvd.earliest_cvd_diagnosis_date,
    COALESCE(smi.has_active_smi_diagnosis, FALSE) AS has_active_smi_diagnosis,
    active_lithium.person_id IS NOT NULL AS is_on_lithium,
    smi.latest_diagnosis_date::DATE AS latest_smi_diagnosis_date,
    smi.latest_remission_date::DATE AS latest_smi_remission_date,
    COALESCE(dyslipidaemia.first_date <= population.reporting_date, FALSE) AS has_dyslipidaemia,
    COALESCE(sleep_apnoea.first_date <= population.reporting_date, FALSE) AS has_obstructive_sleep_apnoea,
    c.earliest_smoking_ltc_diagnosis_date,
    c.earliest_smoking_smi_ltc_diagnosis_date,
    c.earliest_hypertension_date,
    LEAST_IGNORE_NULLS(c.earliest_depression_anxiety_date,
        depression.earliest_child_depression_date) AS earliest_depression_anxiety_date,
    depression.latest_new_depression_diagnosis_date,
    frailty.latest_frailty_severity,
    COALESCE(alcohol_disorder.first_date <= population.reporting_date, FALSE) AS has_alcohol_disorder,
    COALESCE(nice_alcohol_disorder.first_date <= population.reporting_date, FALSE) AS has_nice_alcohol_disorder
FROM population
LEFT JOIN conditions AS c
    ON population.person_id = c.person_id
    AND population.reporting_date = c.reporting_date
LEFT JOIN depression
    ON population.person_id = depression.person_id
    AND population.reporting_date = depression.reporting_date
LEFT JOIN smi
    ON population.person_id = smi.person_id
    AND population.reporting_date = smi.reporting_date
LEFT JOIN frailty
    ON population.person_id = frailty.person_id
    AND population.reporting_date = frailty.reporting_date
LEFT JOIN active_lithium
    ON population.person_id = active_lithium.person_id
    AND population.reporting_date = active_lithium.reporting_date
LEFT JOIN cvd
    ON population.person_id = cvd.person_id
    AND population.reporting_date = cvd.reporting_date
LEFT JOIN alcohol_disorder
    ON population.person_id = alcohol_disorder.person_id
LEFT JOIN nice_alcohol_disorder
    ON population.person_id = nice_alcohol_disorder.person_id
LEFT JOIN dyslipidaemia
    ON population.person_id = dyslipidaemia.person_id
LEFT JOIN sleep_apnoea
    ON population.person_id = sleep_apnoea.person_id
WHERE c.ltc_count > 0
    OR depression.latest_new_depression_diagnosis_date IS NOT NULL
    OR frailty.person_id IS NOT NULL
    OR active_lithium.person_id IS NOT NULL
    OR alcohol_disorder.first_date <= population.reporting_date
    OR nice_alcohol_disorder.first_date <= population.reporting_date
    OR dyslipidaemia.first_date <= population.reporting_date
    OR sleep_apnoea.first_date <= population.reporting_date
{% endmacro %}
