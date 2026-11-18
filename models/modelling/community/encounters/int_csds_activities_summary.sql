{{
    config(
        materialized='table',
        unique_key = 'row_id'
        )
}}

/*
A summary of the number and types of activities per contact

Grain: contact (unique_care_contact_identifier)

Clinical Purpose:
- understanding utilisation across community care

*/

WITH base_join AS (

    SELECT
        bridging.sk_patient_id,
        contact.person_id,
        contact.unique_service_request_identifier AS referral_id,
        contact.unique_care_contact_identifier    AS contact_id,
        activity.unique_care_activity_identifier,
        activity.community_care_activity_type

    FROM {{ ref('stg_csds_care_contact') }} AS contact

    LEFT JOIN {{ ref('stg_csds_cyp202careactivity') }} AS activity
        ON  contact.unique_care_contact_identifier = activity.unique_care_contact_identifier
        AND contact.person_id = activity.person_id      

    LEFT JOIN {{ ref('stg_csds_referral') }} AS referral
        ON  contact.unique_service_request_identifier = referral.unique_service_request_identifier
        AND contact.person_id = referral.person_id

    LEFT JOIN {{ ref('stg_csds_bridging') }} AS bridging 
        ON  contact.person_id = bridging.person_id
),

-- ==========================================================
-- Aggregate to correct grain
-- Each row = unique (patient, referral, contact)
-- ==========================================================
aggregated AS (

    SELECT
        sk_patient_id,
        referral_id,
        contact_id,

        COUNT(DISTINCT unique_care_activity_identifier) AS total_activities_count,
        COUNT_IF(community_care_activity_type = '01') AS count_tests,
        COUNT_IF(community_care_activity_type = '02') AS count_assessments,
        COUNT_IF(community_care_activity_type = '03') AS count_clinical_interventions,
        COUNT_IF(community_care_activity_type = '04') AS count_advice,
        COUNT_IF(community_care_activity_type = '05') AS patient_health_promotion,
        COUNT_IF(community_care_activity_type = '06') AS count_mdt_review,
        COUNT_IF(community_care_activity_type = '07') AS count_clinician_support,
        COUNT_IF(community_care_activity_type IN ('08','09','10','11','12')) AS count_cyp_health_visitor

    FROM base_join
    GROUP BY sk_patient_id, referral_id, contact_id
),

-- ==========================================================
-- Add surrogate key
-- This step references column aliases safely.
-- ==========================================================
final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key([
            "sk_patient_id",
            "referral_id",
            "contact_id"
        ]) }} AS row_id,
        *
    FROM aggregated
)

SELECT * FROM final