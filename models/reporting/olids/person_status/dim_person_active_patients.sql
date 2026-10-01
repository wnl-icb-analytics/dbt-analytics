{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'active'],
        cluster_by=['person_id'])
}}

-- Person Active Patients Dimension Table
-- Uses proper registration data from episode_of_care via int_patient_registrations
-- Filters out deceased patients and dummy patients upstream so the window function
-- and write only process the active cohort.

WITH current_registrations AS (
    -- One row per person with their current registration. is_current_registration
    -- can match more than one row per person (e.g. dual-registered) - pick latest
    -- by registration_start_date as the tie-break.
    SELECT
        ipr.person_id,
        ipr.patient_id,
        ipr.publisher_organisation_id AS current_practice_id,
        ipr.practice_name AS current_practice_name,
        ipr.practice_ods_code AS current_practice_code,
        ipr.registration_start_date AS current_registration_start,
        ipr.registration_duration_days AS current_registration_duration,
        ipr.total_registrations_count,
        ipr.has_changed_practice,
        ipr.practitioner_id
    FROM {{ ref('int_patient_registrations') }} AS ipr
    WHERE ipr.is_current_registration = TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ipr.person_id
        ORDER BY ipr.registration_start_date DESC, ipr.patient_id DESC
    ) = 1
)

SELECT
    dp.person_id,
    p.sk_patient_id,
    dp.patient_ids,
    TRUE AS is_active,
    bd.is_deceased,
    p.is_test_patient,
    p.is_confidential,
    p.is_spine_sensitive,
    p.birth_year,
    p.birth_month,
    bd.death_year,
    bd.death_month,
    cr.current_practice_id,
    cr.current_practice_code,
    cr.current_practice_name,
    cr.current_registration_start,
    cr.current_registration_duration,
    cr.total_registrations_count,
    cr.has_changed_practice,
    cr.practitioner_id,
    p.publisher_organisation_code AS record_owner_org_code,
    p.lds_transform_datetime AS latest_record_date
FROM {{ ref('dim_person') }} AS dp
INNER JOIN current_registrations AS cr
    ON dp.person_id = cr.person_id
INNER JOIN {{ ref('stg_olids_patient') }} AS p
    ON cr.patient_id = p.id
LEFT JOIN {{ ref('dim_person_birth_death') }} AS bd
    ON dp.person_id = bd.person_id
WHERE
    -- Active = alive, not dummy, has a current registration. The INNER JOINs
    -- above enforce "has current registration"; the predicates below enforce
    -- alive + non-dummy. Filtering here means the cluster_by sort and table
    -- write only process the active cohort (~2.2M rows, not 2.4M).
    COALESCE(bd.is_deceased, FALSE) = FALSE
    AND COALESCE(p.is_test_patient, FALSE) = FALSE
