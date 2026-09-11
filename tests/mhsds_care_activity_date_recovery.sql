-- Recovery must preserve supplied contact dates and never invent a clock time.
select count(*) as invalid_activity_dates
from {{ ref('fct_mhsds_care_activity') }} as a
inner join {{ ref('stg_mhsds_care_activity') }} as s on a.mhs202_uniq_id = s.mhs202_uniq_id
left join {{ ref('stg_mhsds_carecontact') }} as c
    on a.uniq_submission_id = c.uniq_submission_id
    and a.referral_source_record_id = c.uniq_serv_req_id
    and a.uniq_care_cont_id = c.uniq_care_cont_id
where (c.care_cont_date is not null and (
        a.care_activity_date is distinct from c.care_cont_date
        or a.care_activity_time is distinct from c.care_cont_time
        or a.care_activity_time_basis is distinct from 'same_submission_care_contact'
    ))
    or (c.care_cont_date is null and (
        a.care_activity_date is distinct from case
            when s.source_derived_activity_date >= '1901-01-01'::date
                and s.source_derived_activity_date between s.reporting_period_start_date and s.reporting_period_end_date
                then s.source_derived_activity_date
        end
        or a.care_activity_time is not null
        or (a.care_activity_date is not null and (
            a.care_activity_time_precision is distinct from 'date'
            or a.care_activity_time_basis is distinct from 'source_derived_activity_date'
            or a.care_activity_at is distinct from a.care_activity_date::timestamp_ntz
        ))
    ))
having count(*) > 0
