select source_record_id, clinical_record_id, source_table, clinical_record_type,
    clinical_at, clinical_time_precision, source_derived_date, is_source_derived_date_inconsistent
from {{ ref('fct_csds_clinical_record') }}
where source_record_id is distinct from clinical_record_id
    or (clinical_time_precision = 'date' and clinical_time is not null)
    or (clinical_at is not null and clinical_time_precision is null)
    or (source_table in ('CYP202', 'CYP612') and clinical_at is not null
        and (is_care_activity_person_consistent is distinct from true
            or is_submitted_contact_person_consistent is distinct from true))
    or (is_source_derived_date_inconsistent is distinct from
        (source_derived_date <> clinical_date))
