{{ config(materialized='view') }}

-- Open registrations for the daily snapshot. EMIS rewrites registration dates
-- after the event, so history rebuilt later differs from the list recorded on
-- the day; the snapshot keeps the recorded list.
select
    registration_record_id,
    person_id,
    practice_ods_code,
    registration_start_date
from {{ ref('int_patient_registrations') }}
where is_current_registration
