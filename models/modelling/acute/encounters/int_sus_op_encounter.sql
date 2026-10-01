/*
Outpatient encounters from SUS

Processing:
- Select key fields
- Rename fields to standard encounter model
- Add dictionary lookups to int_sus_op_min to provide descriptive fields
- Map to known definitions [added later]
- Keeps only attended appointments

Clinical Purpose:
- Establishing use of outpatient services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.

*/

select *
from {{ ref('int_sus_op_appointment') }}
where appointment_attended_or_dna in ('5', '6') 