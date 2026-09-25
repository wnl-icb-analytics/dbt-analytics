/*
Community care encounters (contacts) from CSDS

Clinical Purpose:
- Establishing demand for CSDS
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.
only includes care contacts that were attended

*/

select 
    /* Information needed to derive standard encounter information */
    bridge.sk_patient_id
    , core.clinical_contact_duration_of_care_contact as duration
    , core.care_contact_date as start_date
    , 'CSDS' as source


from {{ ref('stg_csds_care_contact')}} as core

left join {{ ref('stg_csds_bridging')}} as bridge
on core.person_id = bridge.person_id 

where core.attendance_status in ('5', '6')