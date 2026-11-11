/*
Patients whose data may flow to Doccla through the BEVC (DAR067) outbound
share. The share's secure views join each source to this table and return
hx_id in place of person_id or sk_patient_id.

Interim source: Doccla's enrolled-patient file uploads. When Doccla's
enrolment share lands, read its staging model instead and also exclude
patients who have opted out. The share must not go live before then.
*/

with latest_file as (
    select
        hx_id,
        enrolled_date,
        discharged_date,
        file_date
    from {{ ref('stg_myria_enrolled_patients') }}
    qualify file_date = max(file_date) over ()
),

enrolled as (
    select
        hx_id,
        {{ hxflake_to_sk_patient_id('hx_id') }} as sk_patient_id,
        enrolled_date,
        file_date as enrolment_file_date
    from latest_file
    where enrolled_date <= current_date()
        and discharged_date is null
)

select
    enrolled.hx_id,
    enrolled.sk_patient_id,
    pseudo.person_id,
    enrolled.enrolled_date,
    enrolled.enrolment_file_date
from enrolled
-- One row per OLIDS person sharing the patient's sk_patient_id; none if absent
left join {{ ref('dim_person_pseudo') }} as pseudo
    on enrolled.sk_patient_id = pseudo.sk_patient_id
