{{
    config(
        materialized='table')
}}

/*
A summary of the number of referrals per patient

Grain: referral (service_request_identifier)

Clinical Purpose:
- understanding utilisation across community care

*/

SELECT

    bridging.sk_patient_id,
    ARRAY_AGG(DISTINCT referral.primary_reason_for_referral_community_care) AS all_referral_reasons,
    COUNT(referral.unique_service_request_identifier) as referral_count,
    COUNT_IF(referral.service_discharge_date IS NULL) AS count_open_referral,
    COUNT_IF(referral.service_discharge_date IS NOT NULL) AS count_discharged_referral,
    COUNT_IF(referral.priority_type_code = '1') AS  count_routine_priority,
    COUNT_IF(referral.priority_type_code = '2') AS count_urgent_priority,
    COUNT_IF(referral.priority_type_code = '3') AS count_two_week_priority,

FROM
    {{ ref('stg_csds_referral') }} AS referral
LEFT JOIN
    {{ ref('stg_csds_bridging') }} AS bridging 
ON 
    referral.person_id = bridging.person_id
    
GROUP BY
    bridging.sk_patient_id