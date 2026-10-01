{{
    config(
        materialized='table',
        cluster_by=['end_date', 'person_id'],
        tags=['monthly-full']
    )
}}

-- Adults meeting the complexity and utilisation limbs at each month-end.

WITH criteria_inputs AS (
    SELECT
        pm.person_id,
        pm.sk_patient_id,
        pm.month_end_date AS end_date,
        pm.age,

        COALESCE(l.has_af, FALSE) AS has_af,
        COALESCE(l.has_asthma, FALSE) AS has_asthma,
        COALESCE(l.has_chd, FALSE) AS has_chd,
        COALESCE(l.has_ckd, FALSE) AS has_ckd,
        COALESCE(l.has_copd, FALSE) AS has_copd,
        COALESCE(l.has_dementia, FALSE) AS has_dementia,
        COALESCE(l.has_depression, FALSE) AS has_depression,
        COALESCE(l.has_diabetes, FALSE) AS has_diabetes,
        COALESCE(l.has_epilepsy, FALSE) AS has_epilepsy,
        COALESCE(l.has_heart_failure, FALSE) AS has_heart_failure,
        COALESCE(l.has_hypertension, FALSE) AS has_hypertension,
        COALESCE(l.has_smi, FALSE) AS has_smi,
        COALESCE(l.has_stroke_tia, FALSE) AS has_stroke_tia,
        COALESCE(l.has_parkinsons, FALSE) AS has_parkinsons,
        COALESCE(l.has_anxiety, FALSE) AS has_anxiety,
        COALESCE(l.has_learning_disability, FALSE)
            AS has_learning_disability,
        ZEROIFNULL(l.ltc_count) AS ltc_count,

        e.efi_score AS efi2_score,
        e.category AS efi2_category,

        COALESCE(c.has_coded_moderate_severe_frailty, FALSE)
            AS has_coded_moderate_severe_frailty,
        c.latest_frailty_severity,
        c.latest_frailty_date,
        COALESCE(c.is_homeless, FALSE) AS is_homeless,
        COALESCE(c.is_on_palliative_care_register, FALSE)
            AS is_on_palliative_care_register,
        COALESCE(c.has_alcohol_misuse, FALSE) AS has_alcohol_misuse,
        c.latest_alcohol_disorder_date,
        COALESCE(c.has_substance_misuse, FALSE) AS has_substance_misuse,
        c.latest_substance_misuse_date,
        COALESCE(c.is_housebound, FALSE) AS is_housebound,
        c.latest_housebound_status_date,

        ZEROIFNULL(a.gp_appointments_12mo) AS gp_appointments_12mo,
        ZEROIFNULL(a.ed_attendances_12mo) AS ed_attendances_12mo,
        ZEROIFNULL(a.nel_admissions_12mo) AS nel_admissions_12mo,
        ZEROIFNULL(s.outpatient_treatment_functions_12mo)
            AS outpatient_treatment_functions_12mo
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    LEFT JOIN {{ ref('int_segmentation_complex_adults_ltc_history') }} AS l
        ON pm.person_id = l.person_id
        AND pm.month_end_date = l.end_date
    LEFT JOIN {{ ref('fct_person_efi2_by_month') }} AS e
        ON pm.person_id = e.person_id
        AND pm.month_end_date = e.end_date
    LEFT JOIN {{ ref('int_segmentation_clinical_status_history') }} AS c
        ON pm.person_id = c.person_id
        AND pm.month_end_date = c.end_date
    LEFT JOIN {{ ref('int_segmentation_acute_activity_history') }} AS a
        ON pm.person_id = a.person_id
        AND pm.month_end_date = a.end_date
    LEFT JOIN {{ ref('int_segmentation_service_activity_history') }} AS s
        ON pm.person_id = s.person_id
        AND pm.month_end_date = s.end_date
    WHERE pm.is_active AND pm.age >= 18
),

criteria_flags AS (
    SELECT
        *,
        COALESCE(
            efi2_category IN ('MODERATE FRAILTY', 'SEVERE FRAILTY'),
            FALSE
        ) AS has_efi2_moderate_severe_frailty,
        (
            COALESCE(
                efi2_category IN ('MODERATE FRAILTY', 'SEVERE FRAILTY'),
                FALSE
            )
            OR has_coded_moderate_severe_frailty
        ) AS has_moderate_severe_frailty,
        ltc_count >= 3 AS has_3plus_ltcs,
        (
            (ed_attendances_12mo >= 3 OR nel_admissions_12mo >= 2)
            AND gp_appointments_12mo = 0
        ) AS has_high_acute_use_no_gp,
        nel_admissions_12mo >= 2 AS has_2plus_nel_admissions,
        ed_attendances_12mo >= 3 AS has_3plus_ed_attendances,
        gp_appointments_12mo >= 15 AS has_15plus_gp_appointments,
        outpatient_treatment_functions_12mo >= 5
            AS has_5plus_outpatient_treatment_functions
    FROM criteria_inputs
),

criteria_counts AS (
    SELECT
        *,
        (
            has_moderate_severe_frailty::INT
            + has_3plus_ltcs::INT
            + is_homeless::INT
            + is_on_palliative_care_register::INT
            + has_alcohol_misuse::INT
            + has_substance_misuse::INT
            + has_high_acute_use_no_gp::INT
        ) AS complexity_criteria_count,
        (
            has_2plus_nel_admissions::INT
            + has_3plus_ed_attendances::INT
            + has_15plus_gp_appointments::INT
            + has_5plus_outpatient_treatment_functions::INT
            + is_housebound::INT
        ) AS activity_criteria_count
    FROM criteria_flags
)

SELECT *
FROM criteria_counts
WHERE complexity_criteria_count >= 1 AND activity_criteria_count >= 1
