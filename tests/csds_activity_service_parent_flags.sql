-- Historical and latest parent links are different promises. Missing identities stay unknown.
select 'contact' as parent_type, a.source_record_id
from {{ ref('fct_csds_care_activity') }} as a
left join {{ ref('stg_csds_care_contact_history') }} as submitted
    on a.contact_source_row_id = submitted.cyp201_unique_id
    and a.submission_id = submitted.unique_submission_id
    and a.contact_id = submitted.unique_care_contact_identifier
left join {{ ref('fct_csds_care_contact') }} as latest
    on a.contact_source_record_id = latest.source_record_id
where a.is_submitted_contact_linked is distinct from (submitted.cyp201_unique_id is not null)
    or a.is_submitted_contact_person_consistent is distinct from
        case when submitted.cyp201_unique_id is null or a.person_id is null or submitted.person_id is null
            then null else a.person_id = submitted.person_id end
    or a.is_contact_linked is distinct from (latest.source_record_id is not null)
    or a.is_contact_person_consistent is distinct from
        case when latest.source_record_id is null or a.person_id is null or latest.person_id is null
            then null else a.person_id = latest.person_id end
    or a.is_contact_occurrence_consistent is distinct from
        case when submitted.cyp201_unique_id is null or latest.source_record_id is null then null
            else submitted.cyp201_unique_id = latest.source_row_id end

union all

select 'referral' as parent_type, t.source_record_id
from {{ ref('fct_csds_referral_service') }} as t
left join {{ ref('stg_csds_referral_history') }} as submitted
    on t.referral_source_row_id = submitted.cyp101_unique_id
    and t.submission_id = submitted.unique_submission_id
    and t.referral_id = submitted.unique_service_request_identifier
left join {{ ref('fct_csds_referral') }} as latest
    on t.referral_id = latest.source_record_id
where t.is_submitted_referral_linked is distinct from (submitted.cyp101_unique_id is not null)
    or t.is_submitted_referral_person_consistent is distinct from
        case when submitted.cyp101_unique_id is null or t.person_id is null or submitted.person_id is null
            then null else t.person_id = submitted.person_id end
    or t.is_referral_linked is distinct from (latest.source_record_id is not null)
    or t.is_referral_person_consistent is distinct from
        case when latest.source_record_id is null or t.person_id is null or latest.person_id is null
            then null else t.person_id = latest.person_id end
    or t.is_referral_occurrence_consistent is distinct from
        case when submitted.cyp101_unique_id is null or latest.source_record_id is null then null
            else submitted.cyp101_unique_id = latest.source_row_id end
