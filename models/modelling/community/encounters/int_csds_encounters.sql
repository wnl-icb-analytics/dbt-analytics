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
    sk_patient_id
    , clinical_contact_duration_minutes as duration
    , care_contact_date as start_date
    , 'CSDS' as source
from {{ ref('fct_csds_care_contact') }}
where source_attendance_status_code in ('5', '6')