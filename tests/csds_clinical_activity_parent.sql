select r.source_record_id, r.clinical_record_type, r.care_activity_source_record_id,
    r.submission_id, r.clinical_at, a.care_contact_at as expected_clinical_at
from {{ ref('fct_csds_clinical_record') }} as r
left join {{ ref('fct_csds_care_activity') }} as a
    on r.care_activity_source_record_id = a.source_record_id
    and r.submission_id = a.submission_id
left join {{ ref('stg_csds_care_contact_history') }} as c
    on a.submission_id = c.unique_submission_id
    and a.contact_id = c.unique_care_contact_identifier
where r.care_activity_source_record_id is not null
    and (
        r.is_care_activity_linked is distinct from (a.source_record_id is not null)
        or r.is_care_activity_person_consistent is distinct from (r.person_id = a.person_id)
        or r.clinical_at is distinct from case
            when a.is_submitted_contact_linked
                and r.is_care_activity_person_consistent is distinct from false
                and a.is_submitted_contact_person_consistent is distinct from false
                and (r.source_table not in ('CYP610', 'CYP611')
                    or r.person_id is null or c.person_id is null or r.person_id = c.person_id)
                then a.care_contact_at
        end
    )
