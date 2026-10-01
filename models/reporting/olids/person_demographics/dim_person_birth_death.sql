{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'birth_death'],
        cluster_by=['person_id'])
}}

-- Person Birth and Death Dimension Table
-- Core birth and death information for each person
-- Designed to be reused by other dimension tables for age calculations
-- August 2026 - KH amended to catch a nonsense death date of < birth date.

WITH current_patient_per_person AS (
    -- Current registration per person (for SK and current practice context)
    SELECT
        ipr.person_id,
        ipr.patient_id,
        ipr.sk_patient_id
    FROM {{ ref('int_patient_registrations') }} AS ipr
    WHERE ipr.is_current_registration = TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ipr.person_id
        ORDER BY ipr.registration_start_date DESC, ipr.episode_of_care_id DESC
    ) = 1
),

persons_with_patients AS (
    -- Restrict to persons that have at least one mapped patient_id
    SELECT DISTINCT person_id
    FROM {{ ref('int_patient_person_unique') }}
),

patient_candidates AS (
    -- Candidate patient rows per person to source birth/death info
    SELECT
        pp.person_id,
        p.id AS patient_id,
        p.sk_patient_id,
        p.birth_year,
        p.birth_month,
        -- Calculate approximate birth date using exact midpoint of the month
        CASE
        WHEN p.birth_year IS NOT NULL AND p.birth_month IS NOT NULL
            THEN DATEADD(
                DAY,
                FLOOR(
                    DAY(
                        LAST_DAY(
                            TO_DATE(
                                p.birth_year || '-' || p.birth_month || '-01'
                            )
                        )
                    )
                    / 2
                ),
                TO_DATE(p.birth_year || '-' || p.birth_month || '-01')
            )
        END AS birth_date_approx,
        pds.death_year,
        pds.death_month,
        pds.is_deceased,
        pds.death_source_flag,
        pds.death_date_approx,
        p.is_test_patient,
        CASE WHEN p.birth_year IS NOT NULL AND p.birth_month IS NOT NULL THEN 1 ELSE 0 END AS has_dob
    FROM {{ ref('int_patient_person_unique') }} AS pp
    INNER JOIN {{ ref('stg_olids_patient') }} AS p
        ON pp.patient_id = p.id
    LEFT JOIN {{ ref('int_patient_deceased_status') }} AS pds
        ON p.id = pds.patient_id
),

best_patient AS (
    -- Choose a single, best patient per person: prefer one with a DOB
    SELECT *
    FROM patient_candidates
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY has_dob DESC, patient_id DESC
    ) = 1
)
--remove nonsense death dates (death date < birth date)
SELECT
    ap.person_id,
    -- Prefer SK from current registration; otherwise from best mapped patient
    COALESCE(cpp.sk_patient_id, bp.sk_patient_id) AS sk_patient_id,
    bp.birth_year,
    bp.birth_month,
    bp.birth_date_approx,
    CASE WHEN bp.death_date_approx < bp.birth_date_approx THEN NULL ELSE bp.death_year END AS death_year,
    CASE WHEN bp.death_date_approx < bp.birth_date_approx THEN NULL ELSE bp.death_month END AS death_month,
    CASE WHEN bp.death_date_approx < bp.birth_date_approx THEN NULL ELSE bp.death_date_approx END AS death_date_approx,
    CASE WHEN bp.death_date_approx < bp.birth_date_approx THEN FALSE ELSE bp.is_deceased END AS is_deceased,
    CASE WHEN bp.death_date_approx < bp.birth_date_approx THEN NULL ELSE bp.death_source_flag END AS death_source_flag,
    COALESCE(bp.is_test_patient, FALSE) AS is_test_patient
FROM persons_with_patients AS ap
INNER JOIN best_patient AS bp
    ON ap.person_id = bp.person_id
LEFT JOIN current_patient_per_person AS cpp
    ON ap.person_id = cpp.person_id
