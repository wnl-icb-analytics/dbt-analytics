{{
    config(
        tags=['qof', 'point_in_time', 'validation']
    )
}}

/*
QOF Disease Registers - Point in Time

Calculates all QOF disease register counts by practice at a fixed reference date.
Used for QA comparison against static EMIS extracts.

Usage:
    dbt compile --select all_qof_registers_pit --vars '{"qof_reference_date": "2024-03-31"}'

Output Format:
    practice_code | register_name | patient_count | snapshot_date

Notes:
• Uses dim_person_demographics_historical for point-in-time registrations
• Excludes deceased patients as of reference date
• Applies age thresholds at reference date
• Filters all clinical data (diagnoses, medications, observations) to reference date
• Reuses production register business logic via macros

Uses pit_ prefixed views for all 21 QOF disease registers.
*/

WITH reference_date_cte AS (
    SELECT {{ get_reference_date() }} AS reference_date
),

-- =============================================================================
-- SHARED: Active Registrations at Reference Date
-- =============================================================================

active_registrations AS (
    SELECT
        dem.person_id,
        dem.practice_code,
        dem.practice_name,
        dem.pcn_code,
        dem.borough_registered,
        dem.birth_date_approx,
        dem.death_date_approx,
        dem.is_deceased
    FROM {{ ref('dim_person_demographics_historical') }} dem
    CROSS JOIN reference_date_cte rd
    WHERE dem.effective_start_date <= rd.reference_date
      AND (dem.effective_end_date IS NULL OR dem.effective_end_date > rd.reference_date)
      AND dem.is_active = TRUE
      AND (dem.death_date_approx IS NULL OR dem.death_date_approx > rd.reference_date)
),

-- =============================================================================
-- REGISTER CALCULATIONS: Call macros for each register, join to active_registrations
-- =============================================================================

register_diabetes AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_diabetes_register') }} v ON ar.person_id = v.person_id
),

register_asthma AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_asthma_register') }} v ON ar.person_id = v.person_id
),

register_chd AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_chd_register') }} v ON ar.person_id = v.person_id
),

register_hypertension AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_hypertension_register') }} v ON ar.person_id = v.person_id
),

register_copd AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_copd_register') }} v ON ar.person_id = v.person_id
),

register_ckd AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_ckd_register') }} v ON ar.person_id = v.person_id
),

register_atrial_fibrillation AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_atrial_fibrillation_register') }} v ON ar.person_id = v.person_id
),

register_heart_failure AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_heart_failure_register') }} v ON ar.person_id = v.person_id
),

register_stroke_tia AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_stroke_tia_register') }} v ON ar.person_id = v.person_id
),

register_pad AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_pad_register') }} v ON ar.person_id = v.person_id
),

register_dementia AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_dementia_register') }} v ON ar.person_id = v.person_id
),

register_depression AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_depression_register') }} v ON ar.person_id = v.person_id
),

register_smi AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_smi_register') }} v ON ar.person_id = v.person_id
),

register_epilepsy AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_epilepsy_register') }} v ON ar.person_id = v.person_id
),

register_cancer AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_cancer_register') }} v ON ar.person_id = v.person_id
),

register_palliative_care AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_palliative_care_register') }} v ON ar.person_id = v.person_id
),

register_learning_disability AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_learning_disability_register') }} v ON ar.person_id = v.person_id
),

register_osteoporosis AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_osteoporosis_register') }} v ON ar.person_id = v.person_id
),

register_rheumatoid_arthritis AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_rheumatoid_arthritis_register') }} v ON ar.person_id = v.person_id
),

register_ndh AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_qof_ndh_gdm_register') }} v ON ar.person_id = v.person_id
),

register_obesity AS (
    SELECT
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM active_registrations ar
    LEFT JOIN {{ ref('pit_obesity_register') }} v ON ar.person_id = v.person_id
),

-- =============================================================================
-- UNION ALL REGISTERS
-- =============================================================================

all_registers AS (
    SELECT * FROM register_diabetes
    UNION ALL SELECT * FROM register_asthma
    UNION ALL SELECT * FROM register_chd
    UNION ALL SELECT * FROM register_hypertension
    UNION ALL SELECT * FROM register_copd
    UNION ALL SELECT * FROM register_ckd
    UNION ALL SELECT * FROM register_atrial_fibrillation
    UNION ALL SELECT * FROM register_heart_failure
    UNION ALL SELECT * FROM register_stroke_tia
    UNION ALL SELECT * FROM register_pad
    UNION ALL SELECT * FROM register_dementia
    UNION ALL SELECT * FROM register_depression
    UNION ALL SELECT * FROM register_smi
    UNION ALL SELECT * FROM register_epilepsy
    UNION ALL SELECT * FROM register_cancer
    UNION ALL SELECT * FROM register_palliative_care
    UNION ALL SELECT * FROM register_learning_disability
    UNION ALL SELECT * FROM register_osteoporosis
    UNION ALL SELECT * FROM register_rheumatoid_arthritis
    UNION ALL SELECT * FROM register_ndh
    UNION ALL SELECT * FROM register_obesity
)

-- =============================================================================
-- FINAL OUTPUT: Practice-level counts by register
-- =============================================================================

SELECT
    practice_code,
    register_name,
    COUNT(*) FILTER (WHERE is_on_register = TRUE) AS patient_count,
    (SELECT reference_date FROM reference_date_cte) AS snapshot_date
FROM all_registers
GROUP BY practice_code, register_name
ORDER BY practice_code, register_name
