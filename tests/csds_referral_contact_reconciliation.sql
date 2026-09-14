-- Label and person joins must retain each selected source key and attendance code.
select 'referral' as source_entity,
    coalesce(r.unique_service_request_identifier, f.referral_id) as referral_id,
    null::varchar as contact_id,
    iff(r.unique_service_request_identifier is null, 'unexpected_key', 'missing_key') as failure_reason
from {{ ref('stg_csds_cyp101referral') }} as r
full join {{ ref('fct_csds_referral') }} as f on r.unique_service_request_identifier = f.referral_id
where r.unique_service_request_identifier is null or f.referral_id is null
union all
select 'contact', coalesce(c.unique_service_request_identifier, f.referral_id),
    coalesce(c.unique_care_contact_identifier, f.contact_id),
    case when c.unique_care_contact_identifier is null then 'unexpected_key'
        when f.contact_id is null then 'missing_key'
        else 'attendance_changed' end
from {{ ref('stg_csds_cyp201carecontact') }} as c
full join {{ ref('fct_csds_care_contact') }} as f
    on c.unique_service_request_identifier = f.referral_id
        and c.unique_care_contact_identifier = f.contact_id
where c.unique_care_contact_identifier is null or f.contact_id is null
    or c.attended_or_did_not_attend_code is distinct from f.attendance_code
