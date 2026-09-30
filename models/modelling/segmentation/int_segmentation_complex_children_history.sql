{{
    config(
        materialized='table',
        cluster_by=['end_date', 'person_id'],
        tags=['monthly-full']
    )
}}

-- Children meeting at least one complexity criterion at each month-end.

WITH criteria_inputs AS (
    SELECT
        pm.person_id,
        pm.sk_patient_id,
        pm.month_end_date AS end_date,
        pm.age,

        ZEROIFNULL(l.ltc_count) AS ltc_count,
        l.ltc_list,

        ZEROIFNULL(c.complexity_diagnosis_codes)
            AS complexity_diagnosis_codes,
        c.latest_complexity_diagnosis_date,

        ZEROIFNULL(s.paediatric_op_appointments_12mo)
            AS paediatric_op_appointments_12mo,
        ZEROIFNULL(s.outpatient_specialties_12mo)
            AS outpatient_specialties_12mo,
        ZEROIFNULL(s.mh_inpatient_stays_12mo) AS mh_inpatient_stays_12mo,
        ZEROIFNULL(s.community_contacts_12mo) AS community_contacts_12mo,

        cv.community_latest_activity_date,
        cv.is_community_window_complete,
        cv.community_lag_days
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    LEFT JOIN {{ ref('int_segmentation_ltc_count_history') }} AS l
        ON pm.person_id = l.person_id
        AND pm.month_end_date = l.end_date
    LEFT JOIN {{ ref('int_segmentation_clinical_status_history') }} AS c
        ON pm.person_id = c.person_id
        AND pm.month_end_date = c.end_date
    LEFT JOIN {{ ref('int_segmentation_service_activity_history') }} AS s
        ON pm.person_id = s.person_id
        AND pm.month_end_date = s.end_date
    INNER JOIN {{ ref('int_segmentation_service_activity_coverage_history') }} AS cv
        ON pm.month_end_date = cv.end_date
    WHERE pm.is_active AND pm.age < 18
),

criteria_flags AS (
    SELECT
        *,
        ltc_count >= 2 AS has_2plus_ltcs,
        complexity_diagnosis_codes >= 1 AS has_complexity_diagnosis,
        paediatric_op_appointments_12mo >= 5
            AS has_5plus_paediatric_op_appointments,
        outpatient_specialties_12mo >= 2
            AS has_2plus_outpatient_specialties,
        mh_inpatient_stays_12mo >= 1 AS has_mh_inpatient_stay,
        community_contacts_12mo >= 7 AS has_7plus_community_contacts
    FROM criteria_inputs
),

criteria_counts AS (
    SELECT
        *,
        (
            has_2plus_ltcs::INT
            + has_complexity_diagnosis::INT
            + has_5plus_paediatric_op_appointments::INT
            + has_2plus_outpatient_specialties::INT
            + has_mh_inpatient_stay::INT
            + has_7plus_community_contacts::INT
        ) AS complexity_criteria_count
    FROM criteria_flags
)

SELECT *
FROM criteria_counts
WHERE complexity_criteria_count >= 1
