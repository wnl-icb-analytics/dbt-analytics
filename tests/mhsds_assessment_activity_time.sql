select count(*) as inconsistent_inherited_times
from {{ ref('fct_mhsds_clinical_record') }} as r
inner join {{ ref('fct_mhsds_care_activity') }} as a
    on r.care_activity_source_record_id = a.source_record_id
    and r.uniq_submission_id = a.uniq_submission_id
where r.clinical_record_type = 'activity_assessment'
    and r.is_care_activity_person_consistent
    and r.is_care_activity_referral_consistent
    and r.clinical_at is distinct from a.care_activity_at
having count(*) > 0
