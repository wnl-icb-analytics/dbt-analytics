{{
    config(
        materialized='table')
}}

/*
A summary of the conracts and activities per referral

Grain: referral (service_request_identifier)

Clinical Purpose:
- understanding utilisation across community care

*/

WITH activities_per_referral AS (
    SELECT
        a.sk_patient_id,
        a.referral_id,
        SUM(a.count_tests) AS total_tests,
        SUM(a.count_assessments) AS total_assessments,
        SUM(a.count_clinical_interventions) AS total_clinical_interventions,
        SUM(a.count_advice) AS total_advice,
        SUM(a.patient_health_promotion) AS total_health_promotion,
        SUM(a.count_mdt_review) AS total_mdt_reviews,
        SUM(a.count_clinician_support) AS total_clinician_support,
        SUM(a.count_cyp_health_visitor) AS total_health_visitor,
        SUM(a.total_activities_count) AS grand_total_activities
    FROM {{ ref('int_csds_activities_summary') }} AS a
    GROUP BY a.sk_patient_id, a.referral_id
)

SELECT 
    -- contacts
    c.sk_patient_id,
    c.referral_id,
    c.contact_count,
    c.count_seen,
    c.count_dna,
    c.count_patient_cancelled,
    c.count_clinician_cancelled,
    c.average_duration,
    c.count_location_clinical,
    c.count_location_home,
    c.count_location_other,
    -- activities
    a.total_tests,
    a.total_assessments,
    a.total_clinical_interventions,
    a.total_advice,
    a.total_health_promotion,
    a.total_mdt_reviews,
    a.total_clinician_support,
    a.total_health_visitor,
    a.grand_total_activities

FROM {{ ref('int_csds_contacts_summary') }} AS c
LEFT JOIN activities_per_referral AS a
  ON c.referral_id = a.referral_id
 AND c.sk_patient_id = a.sk_patient_id