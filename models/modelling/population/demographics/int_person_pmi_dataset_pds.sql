--Script to get key information from pds data
select
    'pds' as dataset_source,
    sk_patient_id,
    gender_code,
    coalesce(record_person_end_date, current_date()) as gender_event_date,
    year_month_of_birth as date_of_birth,
    coalesce(record_person_end_date, current_date()) as dob_event_date,
    date_of_death,
    coalesce(record_person_end_date, current_date()) as death_event_date,
    preferred_language_code,
    coalesce(record_person_end_date, current_date()) as preferred_language_event_date,
    interpreter_required,
    coalesce(record_person_end_date, current_date()) as interpreter_event_date,
    lsoa_21 as lsoa21_code,
    coalesce(record_residence_end_date, current_date()) as lsoa_event_date,
    practice_code,
    coalesce(record_registered_end_date, current_date()) as registered_event_date
    
from {{ref('int_person_pds_latest_record')}}