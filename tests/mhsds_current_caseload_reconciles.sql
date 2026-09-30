with expected as (
    select count(*) as open_referrals, count(distinct r.person_id) as people
    from {{ ref('fct_mhsds_referral_period') }} as r
    inner join {{ ref('dq_mhsds_provider_submission') }} as p
        on r.provider_organisation_code = p.provider_organisation_code
        and r.reporting_period_end_date = p.reporting_period_end_date
    where p.is_latest_provider_period and r.is_recorded_open_at_period_end
        and r.reporting_period_end_date = p.dataset_latest_reporting_period_end_date
)
select 'open_referrals' as measure, e.open_referrals as expected, a.actual
from expected as e
cross join (select count(*) as actual from {{ ref('fct_mhsds_current_caseload_referral') }}) as a
where e.open_referrals <> a.actual
union all
select 'identifiable_people', e.people, a.actual
from expected as e
cross join (select count(distinct person_id) as actual from {{ ref('fct_mhsds_current_caseload_person') }}) as a
where e.people <> a.actual
