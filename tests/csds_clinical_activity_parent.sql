select r.source_record_id, r.clinical_record_type, r.care_activity_source_record_id,
    r.submission_id, r.clinical_at, a.care_contact_at as expected_clinical_at
from {{ ref('fct_csds_clinical_record') }} as r
left join {{ ref('fct_csds_care_activity') }} as a
    on r.care_activity_source_record_id = a.source_record_id
    and r.submission_id = a.submission_id
where r.care_activity_source_record_id is not null
    and (
        r.is_care_activity_linked is distinct from (a.source_record_id is not null)
        or r.is_care_activity_person_consistent is distinct from (r.person_id = a.person_id)
        or (r.clinical_record_type = 'activity_assessment'
            and r.is_care_activity_person_consistent and r.is_submitted_contact_person_consistent
            and r.clinical_at is distinct from a.care_contact_at)
        or (r.source_table = 'CYP202' and r.is_submitted_contact_person_consistent
            and r.clinical_at is distinct from a.care_contact_at)
    )
